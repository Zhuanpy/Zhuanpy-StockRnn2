import cv2
import numpy as np


# 找图 返回最近似的点
def match_screenshot(img, tmp):
    target = cv2.imread(img)  # 要找的大图
    template = cv2.imread(tmp)  # 图中的小图
    img_gray = cv2.cvtColor(target, cv2.COLOR_BGR2GRAY)
    template_ = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
    result = cv2.matchTemplate(img_gray, template_, cv2.TM_CCOEFF_NORMED)
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
    r = min_val, max_val, min_loc, max_loc
    return r


if __name__ == '__main__':
    target = 'targetfile/screenshot.jpg'
    template = 'targetfile/loginsuccess.jpg'
    data = match_screenshot(target, template)
    print(data)
