import json

#parsed = json.loads(data)
#print(json.dumps(parsed, indent=2, sort_keys=True))

standard = "stand_all.csv"
shredded = "shred_all.csv"

jset = []

def b2kb(e):
  if e.endswith("KB "):
    return float(e.strip(" KB "))
  elif e.endswith("MB "):
    return float(e.strip(" MB "))*1000
  elif e.endswith("B "):
    return float(e.strip(" B "))/1000
  else:
    return e

def rows(e):
  return int(e.strip(" "))

with open(standard, 'r') as f:
  for line in f.readlines():
    eles = line.split("\t")
    jset.append({
      "compilation": "standard",
      "pid": eles[0], 
      "read_size": b2kb(eles[1]), 
      "read_rows": rows(eles[2]),
      "write_size": b2kb(eles[4]),
      "write_rows": rows(eles[5].strip("\n")) })

print(json.dumps(jset, indent=2, sort_keys=True))
