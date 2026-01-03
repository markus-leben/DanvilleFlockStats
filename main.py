import pandas as pd
import math

def isActuallyNan(item):
   try:
      return math.isnan(item)
   except:
      return False


chunk_size = 100000


# csv pulled from https://www.muckrock.com/foi/danville-7714/foia-request-alpr-audit-186112/?ref=404media.co
chunk_iterator = pd.read_csv('rDanville_IL_PD_Network_Audit.csv', chunksize=chunk_size, low_memory=False)


count = 0
instate_count = 0
has_case_count = 0
reasons = {}


for chunk in chunk_iterator:
  for index, row in chunk.iterrows():
    for suffix in ['', '.1', '.2', '.3', '.4', '.5']:
      name = row['Name'+suffix]
      if not isActuallyNan(name):
        count += 1
        reason = str(row['Reason'+suffix]).lower().strip()
        if reason == 'nan':
            reason = '[blank or whitespace]'
        if reason not in reasons:
          reasons[reason] = 1
        else:
          reasons[reason] += 1

        org = str(row['Org Name'+suffix]).strip()
        if 'IL' in org:
          instate_count += 1

        case = str(row['Case #'+suffix]).lower().strip()
        if case != 'nan':
          has_case_count += 1



  print(f"Processing chunk of size: {len(chunk)} rows")

reasons_by_frequency = dict(sorted(reasons.items(), key= lambda tup: tup[1]))
bs_reason_count = 0
for key, value in reasons_by_frequency.items():
   if len(key) < 2 or key in ['info', 'information', 'inv', 'investigation', 'suspect', '[blank or whitespace]', 'suspicious', 'investigations', 'tbd', 'search', 'query', 'investigate', 'training', 'research', 'invst', 'investigaton', '---', 'suspicious activity', 'location', 'suspicous', 'sus', 'sus auto', 'suspicious vehicle', 'testing', 'test', 'inestigation', 'all', 'locations', 'nunya', 'police']:
     bs_reason_count += value
   if value > 100:
    print(f'{key}: {value}')

print('------')
print(f'in state: {instate_count}, {round(100*instate_count/count, 2)}% of total')
print(f'with a case: {has_case_count}, {round(100*has_case_count/count, 2)}% of total')
print(f'with a short, generic, or blank reason: {bs_reason_count}, {round(100*bs_reason_count/count, 2)}% of total')
print(f'total: {count}')