import pygame

import game_functions as gf
from settings import Settings
from ship import Ship


def run_game():
    # 初始化游戏创建屏幕对象
    pygame.init()

    ai_settings = Settings()

    screen = pygame.display.set_mode(
        (ai_settings.screen_width, ai_settings.screen_height))
    pygame.display.set_caption("Alien Invasion")

    # 创建新飞船
    ship = Ship(ai_settings, screen)

    # 开始游戏的主循环
    while True:
        # 监视键盘和鼠标事件
        gf.check_events(ship)
        ship.update()
        gf.update_screen(ai_settings, screen, ship)


run_game()
