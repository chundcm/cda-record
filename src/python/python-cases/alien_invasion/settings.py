
class Settings():
    """存储《外星人入侵》的所有设置的类"""

    def __init__(self):
        """初始化游戏设置"""
        # 屏幕设置
        self.screen_width = 1280
        self.screen_height = 1024
        self.bg_color = (230,230,230)

        # 飞船的速度
        self.ship_speed_factor = 1.5