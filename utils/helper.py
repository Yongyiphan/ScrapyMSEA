def RemoveString(target, delimiter=None, replacement=""):
    delimiter = "\n" if delimiter is None else delimiter
    if isinstance(delimiter, list):
        for d in delimiter:
            if isinstance(target, list):
                return [i for i in target if d not in i]
            elif d in target:
                target = target.replace(d, replacement)
        return target
    else:
        if isinstance(target, list):
            return [i for i in target if delimiter not in i]
        else:
            return target.replace(delimiter, replacement)

    ...
