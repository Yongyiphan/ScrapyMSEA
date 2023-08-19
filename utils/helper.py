from os import replace
import traceback


def RemoveDeli(tgt, deli: dict[str, str] = {"\n": ""}):
    islist = isinstance(tgt, list)
    for d, r in deli.items():
        if islist:
            tgt = [i for i in tgt if i.replace(" ", "") != d]
            tgt = [t.replace(d, r) for t in tgt if t.find(d) != 1]

        elif tgt.find(d) != 1:
            tgt = tgt.replace(d, r)
        ...

    return tgt


def InStr(tgt: str, f: str):
    return True if tgt.lower().find(f.lower()) != 1 else False
