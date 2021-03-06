import json

with open("example_output.json", 'r') as f:
  data = f.read()
  parsed = json.loads(data)
  #print(json.dumps(parsed, indent=2, sort_keys=True))

def mutPerSamp(): 
  out = []
  for sample in parsed:
    name = sample["sample"]
    muts = len(sample["mutations"])
    ele = {"name": name, "amt": muts }
    out.append(ele)
  print(json.dumps(out, indent=2, sort_keys=True))

# this returns 22 for every mutation
# so something is wrong
def samplesPerMutation():
  stats = {}
  for sample in parsed:
    for mut in sample["mutations"]:
      mid = mut["mutId"]
      amt = 1
      if mid not in stats:
        stats[mid] = {"name": mid, "amt": amt}
      else:
        amt = stats[mid]["amt"]
        stats[mid] = {"name": mid, "amt": amt + 1}
  print(json.dumps(stats, indent=2, sort_keys=True))

def mutsPerGene():
  stats = {}
  for sample in parsed:
    for mut in sample["mutations"]:
      for gene in mut["scores"]:
        name = gene["gene"]
        amt = 1
        if name not in stats:
          stats[name] = {"name": name, "amt": amt}
        else:
          amt = stats[name]["amt"]
          stats[name] = {"name": name, "amt": amt+1 }
  print(json.dumps(stats, indent=2, sort_keys=True))

# run report here  
mutPerSamp()
#samplesPerMutation()  
mutsPerGene()
