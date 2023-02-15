import pandas as pd
import re
from colorama import Fore, Back, Style
import plotly as py
import plotly.figure_factory as ff
import plotly.express as px

# 使pandas的console文字輸出可以對齊(中文適用)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


class Group:
    """
    用來處理與紀錄有序且循環的一維list。
    定義一個group:list=[task_id,task_id,...]
    +=1表示清單的ptr指向下一個元素，並且在最後一個元素後將指向第一個元素。
    使用(Group實體).ptr可以回傳目前指向的元素。(getter)
    使用(Group實體).status:可以變更或回傳此group的狀態。(getter & setter)
    定義Dependency迭代器為groups:dict迭代器，實現如dict使用for可取得keys。
    """

    def __init__(self, group: list = [], index: int = 0, status: str = 'GROUP-等待排程') -> None:
        self.__group: list = group
        self.__index: int = index
        self.__status: str = status

    # 自定義物件加減法，實現環狀清單
    def __add__(self, quantity: int):
        if isinstance(quantity, int):
            self.__index += quantity
            self.__index %= len(self.__group)
            return self
        else:
            raise TypeError('quantity must be an interger')

    # 自定義物件加減法，實現環狀清單
    def __sub__(self, quantity: int):
        if isinstance(quantity, int):
            self.__index -= quantity
            self.__index %= len(self.__group)
            return self
        else:
            raise TypeError('quantity must be an interger')

    # 自定義物件迭代器指向self.__group
    def __iter__(self):
        return iter(self.__group)

    # 限制唯讀
    @property
    def tasks(self):
        return list(self.__group)

    # 限制唯讀
    @property
    def ptr(self):
        return int(self.__group[self.__index])

    # 限制唯讀
    @property
    def status(self):
        return str(self.__status)

    # 限制賦值方式
    @status.setter
    def status(self, status):
        self.__status = status


class Dependency:
    """
    相依性處理器。
    定義一個groups:dict={'規則 1:Group(),'規則 2':....}。
    定義Dependency迭代器為groups:dict迭代器，實現如dict使用for可取得keys。
    """

    # 初始化
    def __init__(self, dependency: dict = {}) -> None:
        self.__dependency: dict = dependency
        self.__groups: dict = {}

        for key in self.__dependency:
            self.__groups[key] = Group(group=self.__dependency[key])

    # 自定義print()格式
    def __str__(self) -> str:
        show = '\n'
        for key in self.__groups:
            show += str(key)+':\t['
            for task in self.__groups[key].tasks:
                if task != self.__groups[key].ptr:
                    show += str(task)
                else:
                    show += Fore.BLACK + Back.WHITE + \
                        Style.BRIGHT + str(task)+Style.RESET_ALL
                if task != self.__groups[key].tasks[-1]:
                    show += ', '
                else:
                    show += ']\n'
            show += str(self.__groups[key].status) + \
                '\n\n'
        return str(show)

    # 自定義取值運算子[]被調用取值時，回傳self.__groups[key]值。(應該會回傳一個Group物件)
    def __getitem__(self, key):
        return self.__groups[key]

    # 自定義取值運算子[]被調用進行賦值時，對接上欲寫入的變數位置。(應該會覆寫被修改過的Group物件到dict內)
    def __setitem__(self, key, value):
        self.__groups[key] = value

    # 自定義迭代器指向self.__groups:dict，值為dict.keys()。
    def __iter__(self):
        return iter(self.__groups)


class Timeline:
    """
    時間線處理器。
    +=1可以使ptr前進1*單位時間(unit)。
    使用(Timeline實體).ptr可以回傳目前的時間戳。(getter)
    """

    def __init__(self, **timeline) -> None:
        self.__start: float
        self.__end: float
        self.__ptr: float
        self.__unit: float

        if isinstance(timeline, dict):
            try:
                self.set_timeline(
                    timeline['start'], timeline['end'], timeline['ptr'], timeline['unit'])
            except:
                raise

    def __str__(self):
        return str(self.to_dict())

    # 自訂義加法
    def __add__(self, quantity: int):
        if isinstance(quantity, int):
            self.__ptr += quantity*self.__unit
            if self.__ptr > self.__end:
                self.__ptr = self.__end
            elif self.__ptr < self.__start:
                self.__ptr = self.__start
            return self
        else:
            raise TypeError('quantity must be an interger')

    # 限制唯讀
    @property
    def start(self):
        return self.__start

    # 限制唯讀
    @property
    def end(self):
        return self.__end

    # 限制唯讀
    @property
    def ptr(self):
        return self.__ptr

    # 限制唯讀
    @property
    def unit(self):
        return self.__unit

    def set_timeline(self, start: float = 0.0, end: float = 0.0, ptr: float = 0.0, unit: float = 1.0) -> None:
        parameter_error = False
        if start > end:
            parameter_error = True
            print('[fail to set timeline]:start time must after end time!')
        if ptr > end or ptr < start:
            parameter_error = True
            print('[fail to set timeline]:ptr must in between start time and end time!')

        if not parameter_error:
            self.__start = float(start)
            self.__end = float(end)
            self.__ptr = float(ptr)
            self.__unit = float(unit)

    def is_end(self) -> bool:
        # 提醒：self.__ptr已被operator:'+'限制不可超過self.__end，因此此處的判斷條件式一定要使用'=' or '>='；如果使用'>'會導致迭代的無窮迴圈
        return True if self.__ptr >= self.__end else False

    def to_dict(self) -> dict:
        return {'start': self.__start, 'end': self.__end, 'ptr': self.__ptr, 'unit': self.__unit}


class Scheduler(Timeline):
    def __init__(self, **scheduler) -> None:
        # 任務類型表。格式:{動作編號:[動作名稱,運行模式,運行時間,不良率,參考產量]}
        self.__tasks: dict = {}
        # 相依性處理器。
        self.__dependency: Dependency = Dependency()
        # 任務排程註冊表。格式:pd.DataFarme{index:[動作編號],col:[狀態, 時效戳, 註冊戳, 鎖定者]}
        self.__registry: pd = pd.DataFrame()
        # 紀錄檔。
        self.__records: dict = {}

        if isinstance(scheduler, dict):
            try:
                super().__init__(**scheduler['timeline'])
                self.__init(scheduler)
            except:
                raise

    def __init(self, scheduler):
        timeline_ptr = scheduler['timeline']['ptr']

        self.__tasks = scheduler['tasks']
        self.__dependency = Dependency(scheduler['dependency'])

        belonging_list = []
        for task in self.__tasks:
            tmp = []
            for group_name in scheduler['dependency']:
                if task in scheduler['dependency'][group_name]:
                    tmp.append(group_name)

            belonging_list.append(tmp)
        df = pd.DataFrame({
            '動作編號': self.__tasks.keys(),
            '狀態': ['TASK-等待排程']*len(self.__tasks.keys()),
            '時效戳': [float(timeline_ptr)]*len(self.__tasks.keys()),
            '註冊戳': [float(timeline_ptr)]*len(self.__tasks.keys()),
            '群組': belonging_list
        })

        self.__registry = df.set_index('動作編號')
        self.__records = dict.fromkeys(
            self.__tasks.keys(), pd.DataFrame(columns=['狀態', '起始時間', '結束時間']))

    def __str__(self):
        df = self.__registry.reset_index()
        timeline_ptr = int(
            round(((self.ptr-self.start)/(self.end-self.start))*40))
        pass

        line = '~~~~~~~~~~~~~~~~~~~~~~~~分隔線~~~~~~~~~~~~~~~~~~~~~~~~\n'
        col_0 = '時間軸:'
        timeline = '|'+(' ' * (timeline_ptr-1))+'☆'+(' '*(40-timeline_ptr))+'|\n|'+'*' * \
            40+'|\n'+str(self.start)+(' '*(42-len(str(self.start)) -
                                           len(str(self.end)))+str(self.end))
        timer = '\nTimer: At '+str(self.ptr)+' sec\n'
        col_1 = '註冊表:'
        registry = '\n' + df.to_string(index=False)+'\n'
        col_2 = '群組狀態:'
        dependency = str(self.__dependency)

        col_0 = '\n'+Fore.BLUE+Back.YELLOW+Style.BRIGHT+col_0+Style.RESET_ALL+'\n'
        col_1 = '\n'+Fore.BLUE+Back.YELLOW+Style.BRIGHT+col_1+Style.RESET_ALL+'\n'
        col_2 = '\n'+Fore.BLUE+Back.YELLOW+Style.BRIGHT+col_2+Style.RESET_ALL+'\n'

        s = line+col_0+timeline+timer+col_1+registry+col_2+dependency

        return s

    # 唯讀
    @property
    def tasks(self):
        return self.__tasks

    @property
    def dependency(self):
        return self.__dependency

    @property
    def rulegroups(self):
        return self.__rulegroups

    @property
    def registry(self):
        return self.__registry

    @property
    def records(self):
        return self.__records

    # 註冊
    def regist(self, tasks_id: list = [], status: str = 'TASK-等待排程', checkpoint_timestamp: float = None) -> None:
        if isinstance(tasks_id, list):
            for task in tasks_id:
                self.__registry.loc[task, '狀態'] = status
                self.__registry.loc[task, '時效戳'] = checkpoint_timestamp
                self.__registry.loc[task, '註冊戳'] = self.ptr
                # self.__registry.loc[task, '群組'] = group
        elif isinstance(tasks_id, int):
            task = tasks_id
            self.__registry.loc[task, '狀態'] = status
            self.__registry.loc[task, '時效戳'] = checkpoint_timestamp
            self.__registry.loc[task, '註冊戳'] = self.ptr
            # self.__registry.loc[task, '群組'] = group

    # 計算
    def calc(self):
        init_flag = True
        # 計時器啟動
        while not self.is_end():
            """ 規則群組檢查當前狀態與執行指針移動，檢查與解鎖到期程序的鎖定狀態 """
            # 製做狀態遮罩
            mask_status_1 = self.__registry['狀態'] == 'TASK-運行中'
            mask_status_2 = self.__registry['狀態'] == 'TASK-排程鎖定'
            mask_status_3 = self.__registry['狀態'] == 'TASK-凍結'
            mask_status_4 = self.__registry['狀態'] == 'TASK-等待排程'
            mask_checkpoint_1 = self.__registry['時效戳'] == self.ptr
            mask_checkpoint_2 = self.__registry['時效戳'] > self.ptr

            # 所有的tasks_id
            all_tasks = self.__registry.index.tolist()
            # 本回合已結束的tasks_id
            finished_tasks = self.__registry.index[mask_status_1 & mask_checkpoint_1].tolist(
            )
            # 正在運行的tasks_id
            executing_tasks = self.__registry.index[mask_status_1 & mask_checkpoint_2].tolist(
            )
            # 被正在運行的tasks鎖定的tasks
            waiting_locked_tasks = self.__registry.index[mask_status_2].tolist(
            )
            # 被凍結的tasks_id
            freezed_tasks = self.__registry.index[mask_status_3].tolist()
            # 上一回合結束卻沒有進入排程or被鎖定的tasks_id(異常)
            error_tasks = self.__registry.index[mask_status_4].tolist()

            # [測試] 排程異常，印出狀態總表
            if error_tasks and not init_flag:
                print(self)
                raise

            # 當本回合有tasks完成執行之時
            if finished_tasks or init_flag:
                init_flag = False

                snapshot_st = pd.DataFrame(self.__registry.to_dict())

                # 解除所有本回合運行中以外的排程鎖定
                unlocked_tasks = list(
                    set(all_tasks).difference(set(executing_tasks)))
                self.regist(tasks_id=unlocked_tasks, status='TASK-等待排程')

                # 把結束的tasks告訴ptr in tasks的群組已執行完畢，進入閒置狀態
                # 遊覽整個GROUP，解除所有凍結狀態。
                for group in self.__dependency:

                    if self.__dependency[group].status == 'GROUP-凍結':
                        # 解除群組閒置狀態
                        self.__dependency[group].status = 'GROUP-等待排程'

                    elif self.__dependency[group].status == 'GROUP-運行中':
                        # 本回合剛完成的排程
                        if self.__dependency[group].ptr in finished_tasks:
                            # 解除群組運行狀態
                            self.__dependency[group].status = 'GROUP-等待排程'
                            # 指標指向群組內的下一個task
                            self.__dependency[group] += 1

                        # 本回合內還在運行中的排程
                        else:
                            group_tasks = self.__dependency[group].tasks
                            group_tasks.remove(self.__dependency[group].ptr)
                            # 重新鎖定排程鎖
                            self.regist(tasks_id=group_tasks,
                                        status='TASK-排程鎖定')

                # 確認排程啟動條件，剩餘程序進行凍結
                for group in self.__dependency:
                    if self.__dependency[group].status == 'GROUP-等待排程':
                        if self.__registry['狀態'][self.__dependency[group].ptr] == 'TASK-等待排程' or self.__registry['狀態'][self.__dependency[group].ptr] == 'TASK-運行中':
                            self.__dependency[group].status = 'GROUP-運行中'
                            # 將群組內的所有tasks註冊排程鎖定
                            self.regist(
                                tasks_id=self.__dependency[group].tasks, status='TASK-排程鎖定')
                            # ptr覆蓋TASK-排程鎖定狀態，改為TASK-運行中狀態，並加入完成時間點
                            self.regist(
                                tasks_id=self.__dependency[group].ptr, status='TASK-運行中', checkpoint_timestamp=self.ptr + self.__tasks[self.__dependency[group].ptr][2])
                        elif self.__registry['狀態'][self.__dependency[group].ptr] == 'TASK-排程鎖定' or self.__registry['狀態'][self.__dependency[group].ptr] == 'TASK-凍結':
                            self.__dependency[group].status = 'GROUP-凍結'
                            for task in self.__dependency[group].tasks:
                                if self.__registry['狀態'][task] == 'TASK-等待排程':
                                    self.regist(tasks_id=task,
                                                status='TASK-凍結')

                # 紀錄
                snapshot_nd = pd.DataFrame(self.__registry.to_dict())
                self.record(snapshot_st, snapshot_nd)

                # [測試] 當本回合有異動，就印出狀態總表
                # print(self)

            # 當本回合沒有遇到Task完成的事件時，則休眠
            else:
                pass

            # 計時器迭代
            self += 1

    # 紀錄tasks運行狀態變化
    def record(self, before, after):
        next_checkpoint = min(self.__registry['時效戳'].dropna())
        for task_id in self.__tasks.keys():

            if before.at[task_id, '狀態'] == after.at[task_id, '狀態']:
                self.__records[task_id]['結束時間'].iat[-1] = next_checkpoint
            else:
                s = pd.DataFrame(
                    {'狀態': [after.at[task_id, '狀態']], '起始時間': [self.ptr], '結束時間': [next_checkpoint]})
                self.__records[task_id] = pd.concat(
                    [self.__records[task_id], s], axis=0, ignore_index=True)

    def to_gannta(self):
        # 創建一個空的plotly.gantt參數格式的datafeame
        df = pd.DataFrame({'Task': [], 'Start': [], 'Finish': [], 'Status': []})
        # 讀取紀錄並轉換格式
        for key in self.records:
            col = {'狀態': 'Status', '起始時間': 'Start', '結束時間': 'Finish'}
            d = pd.DataFrame(self.records[key].to_dict())
            # d = d[d['狀態'] != 'TASK-排程鎖定']
            
            # 備註：self.__tasks[key][0]對應到task的name欄位
            d['Task'] = self.__tasks[key][0]

            d = d.rename(columns=col)
            df = pd.concat([df, d], ignore_index=True)

        df['Start']=pd.to_datetime(df['Start'],unit='s')
        df['Finish']=pd.to_datetime(df['Finish'],unit='s')
        print(df)
        fig=ff.create_gantt(df,group_tasks = True, index_col = 'Status', show_colorbar=True)
        pyoff = py.offline.plot
        pyoff(fig, filename = 'test.html')

actionList_filename = '動作.csv'
dependency_filename = '相依.txt'

actions: dict = {}
dependency: dict = {}
idle_regristry: dict = {}
processes = {'Task': [], 'Status': [], 'Start': [], 'Finish': []}
scheduler: Scheduler = Scheduler


def check_and_open_CSVfile(filepath: str = '') -> pd:
    for decode in ('gbk', 'utf-8', 'gb18030', 'ISO-8859-1'):
        try:
            df = pd.read_csv(filepath, encoding=decode,
                             on_bad_lines='skip')
            print('data-' + decode + '-success!!')
            return df
        except:
            pass


def IO_actions(filename: str = '') -> None:
    global actions
    if filename == '':
        filename = actionList_filename
    df = check_and_open_CSVfile(filepath=filename)

    df = df.set_index('動作編號')
    index = df.index
    value = df.values.tolist()

    actions = dict(zip(index, value))


def IO_dependency(filename: str = '') -> None:
    global dependency, actions
    if filename == '':
        filename = dependency_filename
    with open(filename) as f:
        for counter, line in enumerate(f.readlines()):
            # 相依表內創建子分類:group 1, group 2...
            dependency['群組 '+str(counter+1)] = []
            # 以逗號、空白分隔字串
            tmp = re.split(r',|\s', line)
            # subject代表的是action的ID
            for subject in tmp:
                # 排除空字串:''
                if subject:
                    # dependency['群組 i']:list=[]
                    dependency['群組 '+str(counter+1)].append(int(subject))


if __name__ == '__main__':
    IO_actions()
    IO_dependency()
    timeline = {'start': -50, 'end': 500, 'ptr': 0, 'unit': 1}

    sc = Scheduler(timeline=timeline, tasks=actions, dependency=dependency)
    sc.calc()
    sc.to_gannta()
