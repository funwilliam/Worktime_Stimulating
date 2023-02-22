from ctypes import pointer, Structure, windll
from ctypes.wintypes import WORD, SHORT, SMALL_RECT

#定義結構
class COORD(Structure):
    _fields_ = [("X", SHORT), ("Y", SHORT)]

    def __init__(self, x, y):
        self.X = x
        self.Y = y


class CONSOLE_SCREEN_BUFFER_INFO(Structure):
    _fields_ = [("dwSize", COORD), ("dwCursorPosition", COORD),
                ("wAttributes", WORD), ("srWindow", SMALL_RECT),
                ("dwMaximumWindowSize", COORD)]

class cursor:
    """
    console光標控制器：gotoxy(x, y), getxy(x, y)
    """
    # console的cursor位置指定
    def gotoxy(x, y):
        hOut = windll.kernel32.GetStdHandle(-11)
        windll.kernel32.SetConsoleCursorPosition(hOut, COORD(x, y))

    # console的cursor位置讀取
    def getxy():
        csbi = CONSOLE_SCREEN_BUFFER_INFO()
        hOut = windll.kernel32.GetStdHandle(-11)
        windll.kernel32.GetConsoleScreenBufferInfo(hOut, pointer(csbi))
        return csbi.dwCursorPosition.X, csbi.dwCursorPosition.Y