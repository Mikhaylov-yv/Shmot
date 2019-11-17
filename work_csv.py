import os.path
import pandas as pd
import sys

def open_csv(fil_name):
    if os.path.exists(fil_name) == True:
        data = pd.read_excel(fil_name)
        if len(data) != 0:
            return data
        else: return 0
    else:
        print('Файла нет!!!')
        sys.exit()


