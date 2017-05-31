# coding: utf-8

import re
import json


def filterkeywords(*args):

    """
    .. function:: filterkeywords(str, jgroup) -> str

    Returns the input text with the stopwords removed. The case of the first letter matters.

    Examples:

    >>> table1('''
    ... 'this and wood'         'NO more No words'
    ... 'No more stop words'    'more free time'
    ... ''')
    >>> sql("select filterkeywords(a, b) from table1")
    filterkeywords(a,b)
    --------------------
    wood NO words
    stop words free time
    """

    keywords = json.loads(args[-1])

    if len(args) == 2:
        return ' '.join([k for k in args[0].split(' ') if k!='' and k[0].lower()+k[1:] not in keywords])

    # out=[]
    # for i in args:
    #     out.append(' '.join([k for k in i.split(' ') if k!='' and k[0].lower()+k[1:] not in stopwords]))
    #
    # return ' '.join(out)

filterkeywords.registered=True


if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import src.functions.row.setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()
