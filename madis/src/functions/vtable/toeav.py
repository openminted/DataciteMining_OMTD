"""

.. function:: fromeav(query) -> Relational table

Transforms the query input results to a relational table from an Entity-Attribute-Value (EAV) model.

Examples::

    >>> table1('''
    ... 1    name    James
    ... 1    city    Chicago
    ... 1    job    Programmer
    ... 1    age    35
    ... 2    name    Mark
    ... 2    city    London
    ... 2    job    Pilot
    ... 2    age    43
    ... 5    name    Lila
    ... 5    city    'New York'
    ... 5    job    Teacher
    ... 5    age    29
    ... ''')

    >>> sql("fromeav select * from table1 where a = 1")
    _rowid_ | name  | city    | job        | age
    --------------------------------------------
    1       | James | Chicago | Programmer | 35

    >>> sql("fromeav select * from table1 where a in (1,3)")
    _rowid_ | name  | city     | job        | age
    ---------------------------------------------
    1       | James | Chicago  | Programmer | 35
    3       | Lila  | New York | Teacher    | 29

    >>> sql("fromeav select * from table1")
    _rowid_ | name  | city     | job        | age
    ---------------------------------------------
    1       | James | Chicago  | Programmer | 35
    2       | Mark  | London   | Pilot      | 43
    3       | Lila  | New York | Teacher    | 29

"""
import setpath
import vtbase
import functions
import gc

### Classic stream iterator
registered=True

class fromEAV(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument")
        query = dictargs['query']

        cur = envars['db'].cursor()
        c = cur.execute(query, parse=False)
        schema = [('_rowid_',)]
        schemaorder = {}
        record = []
        r1=[]
        firstrow = c.next()
        schema.append((firstrow[1],))
        record.append(firstrow[0])
        record.append(firstrow[2])
        for row in c:
            if row[0] == firstrow[0]:
                schema.append((row[1],))
                record.append(row[2])
            else:
                firstrow=row
                r1.append(row[0])
                r1.append(row[2])
                break
        yield schema
        yield record
        
        record=r1
        for row in c:                    
            if row[0] == firstrow[0]:
                record.append(row[2])
            else:
                firstrow=row
                r1=[]
                r1.append(row[0])
                r1.append(row[2])
                yield record
                record=r1
        yield record
        

def Source():
    return vtbase.VTGenerator(fromEAV)

if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()
