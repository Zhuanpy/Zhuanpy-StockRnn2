
from parser_utils import read_columns

# "Boll": {"1": "BollMid", "2": "BollStd", "3": "BollUp", "4": "BollDn", "5": "StopLoss"}
col = read_columns()
BollMid = col['Boll']['1']
BollStd = col['Boll']['2']
BollUp = col['Boll']['3']
BollDn = col['Boll']['4']
StopLoss = col['Boll']['5']
