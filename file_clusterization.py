from pathlib import Path

#File name normalization. Split into words. 
def split_name(filepath:str) -> list:
    """
    Split a file name (without extension) into words.
    Words are separated by non-alphabetic characters like '-' or '_'.
    """
    filepath = Path(filepath)
    words = []
    if filepath.is_file():
        name_without_suffix = filepath.name.removesuffix(filepath.suffix)
        current_word = ''
        for char in name_without_suffix:
            if char .isalpha():
                current_word +=char 
            if char in ['-', '_']:
                words.append(current_word)
                current_word  = ''
        if current_word != '':
            words.append(current_word)
    return words 

# Accept only unique sequences
def unique_sequences(tokens:list) -> list:
    """
    Return a list of unique tokens, preserving order.
    """
    seen = set()
    unique_tokens = []

    for token in tokens:
        if token not in seen:
            seen.add(token)
            unique_tokens.append(token)

    return unique_tokens

# For each sequence select reprezentative word (one element)
def find_repr_word(groups:list) -> list:
    """
    For each group of tokens, select a representative word
    that is unique to that group.
    """
    group_sets = [set(g) for g in groups]
    representatives = []

    for i, current in enumerate(group_sets):
        others = set().union(*[g for j, g in enumerate(group_sets) if j != i])
        unique_tokens = current - others

        # pick one representative (arbitrary but unique)
        representatives.append(next(iter(unique_tokens)) if unique_tokens else None)
        
    return representatives

# function accepts list o paths to files (or just filenames)
# returns list of filenames with relative paths (with structurised parent folder)
def cluster_files(files:list) -> list:
    # Only keep existing files
    files = [f for f in files if f.is_file()]
    if not files:
        return []
    
    name_tokens = [split_name(file) for file in files]
    unique_tokens = unique_sequences(name_tokens)
    representatives = find_repr_word(unique_tokens)
    
    clustered_paths = []
    
    for file in files:
        filename = file.name
        for token in representatives:
            if token and token in filename:
                clustered_paths.append(Path(token) / filename)
                continue
    
    return clustered_paths