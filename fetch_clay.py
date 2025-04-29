import requests
from time import sleep
import pandas as pd

ruts = ['76134123',
        '76211029',
        '76285545',
        '76361422'
        ]
api_keys = ['KXeZHL6Cpych_QgII7Yes-wboXFHnZIw6QO8zevyivR_uNQYYxhKjW2t0hLe0V9UL7xch9aAhovUdobf4LqE8B',
            'GrR-X_7dMO7mxCQk0MREpPqloBuwGeEPx1KuncjRzFDiYSH8f9Qqps40Pt2YVwRLsYuIeABfBM4w_jLfPnMGSt',
            's70GUQKyUgwu7WFwtwhzNtEUFSzHplL6IdFIKl_elzhlqnuzMgwplUETtaP6j5uvYTJwHnAPztO33UyIPuiwoF',
            '-5XKfP1Mk7XqsBDPKZjzXP2SOP0MkMyi9Maaatq38aW-EMfdu9TgEJKnlNmvNLxbxForN14sarZ8EjDf_zNH9j'
            ]

acc_nums = [[11481052701, 1481052700],
        [5100539286, 66807428],
        [27900031110, 34770806293],
        [76987299]]

def get_all_matches(rut, num, key):
  result = query(rut,num,key,0)
  if not result:
    return None
  total_records = result["data"]["records"]["total_records"]
  items = result["data"]["items"]
  offset = 200
  while offset < total_records:
    sleep(2)
    result = query(rut,num,key,offset)
    items.extend(result["data"]["items"])
    offset += 200
  return items

def query(rut,num,key,offset):
  url = f"https://api.clay.cl/v1/cuentas_bancarias/matches/?abono=true&orden=Asc&numero_cuenta={num}&rut_empresa={rut}&limit=200&offset={offset}&fecha_match_desde=2020-01-01&fecha_match_hasta=2025-04-24&fecha_desde=2020-01-01&fecha_hasta=2025-04-24"
  headers = {
    'accept': 'application/json',
    'Token': key
  }
  response = requests.get(url, headers=headers)

  if response.status_code == 200:
      return response.json()

  else:
      print(f"Request failed with status code {response.status_code}: {response.text}")
      return None

items = []
for i in range(len(ruts)):
  rut = ruts[i]
  nums = acc_nums[i]
  key = api_keys[i]
  for num in nums:
    matches = get_all_matches(rut,num,key)
    if matches:
      items.extend(matches)
  sleep(2)

df = df = pd.json_normalize(items, sep='_')
#items["folio", descr, "emisor_obligacion.rut+dv", "receptor_obligacion.rut+dv", monto_original_movimiento, monto_match, ]
df.to_csv("Clay.csv", index=False)