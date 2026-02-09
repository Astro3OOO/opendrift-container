from pathlib import Path

#File name normalization. Split into words. 
def SplitName(pth:str) -> list:
    pth = Path(pth)
    res = []
    if pth.is_file():
        n = pth.name.removesuffix(pth.suffix)
        txt = ''
        for l in n:
            if l.isalpha():
                txt+=l
            if l in ['-', '_']:
                res.append(txt)
                txt = ''
        if txt != '':
            res.append(txt)
    return res

# Accept only unique sequences
def Cut(tokens:list) -> list:
    groups = []
    for token in tokens:
        if token not in groups:
            groups.append(token)
    return groups

# For each sequence select reprezentative word (one element)
def ReprWords(groups:list) -> list:
    group_sets = [set(g) for g in groups]

    representatives = []

    for i, current in enumerate(group_sets):
        others = set().union(*[g for j, g in enumerate(group_sets) if j != i])
        unique_tokens = current - others

        # pick one representative (arbitrary but unique)
        if unique_tokens:
            representatives.append(next(iter(unique_tokens)))
        else:
            representatives.append(None) 
    return representatives

# function accepts list o paths to files (or just filenames)
# returns list of filenames with relative paths (with structurised parent folder)
def ClusterFiles(files:list) -> list:
    if not all([Path(file).is_file() for file in files]):
        return []
    
    names = [SplitName(file) for file in files]
    groups = Cut(names)
    target = ReprWords(groups)
    
    paths = []
    
    for file in files:
        name = file.name
        for token in target:
            if token in name:
                pth = Path(token) / name
                paths.append(pth)
                continue
    
    return paths