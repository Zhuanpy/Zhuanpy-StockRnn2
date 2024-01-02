import json
from root_ import file_root


def read_columns():
    _path = file_root()  # 获取root file 路径
    path_ = f'{_path}/pp/StockColumns.json'

    with open(f'{path_}', 'r') as f:
        col_ = json.load(f)

    return col_


if __name__ == '__main__':
    p = file_root()
    print(p)
