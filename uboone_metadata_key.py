
def metadataList(expname):
    return [expname[:2] + elt for elt in ('ProjectName', 'ProjectStage', 'ProjectVersion')]


def translateKey(key):
    prefix = key[:2]
    stem = key[2:]
    projNoun = stem.split("Project")
    return prefix + "_Project." + projNoun[1]