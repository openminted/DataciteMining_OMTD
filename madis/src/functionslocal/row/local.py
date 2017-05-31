import json

def c2j(*args):

    """
    .. function:: t2j(tabpack) -> jpack

    Converts a tab separated pack to a jpack.

    Examples:

    >>> sql("select t2j(j2t('[1,2,3]'))") # doctest: +NORMALIZE_WHITESPACE
    t2j(j2t('[1,2,3]'))
    -------------------
    ["1","2","3"]

    >>> sql("select t2j('asdfasdf')") # doctest: +NORMALIZE_WHITESPACE
    t2j('asdfasdf')
    ---------------
    ["asdfasdf"]

    """
    
    fj=[]
    for t in args:
        fj+=t.split(',')

    return json.dumps(fj, separators=(',',':'), ensure_ascii=False)

c2j.registered = True
