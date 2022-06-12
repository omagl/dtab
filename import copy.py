import copy
from typing import List
import json

class Tod:
    data:List

    def join(self, other, on:list, type:str='inner', suffix:str="1", prefix:str=None, keepkeys:bool=False, renameright:bool=False) ->  list:
        """Join two dict-tables (list of dicts)

        Args:
            left (list): table 1
            right (list): table 2
            on (list): keys used for join
            type (str, optional): join type, 'inner' or 'left' Defaults to 'inner'.
            suffix (str, optional): suffix if right column has the same name as left column. Defaults to "1".
            prefix (str, optional): prefix if the right column has the same name as left column. Defaults to None.
            keepkeys (bool, optional): keep join keys in right but renamed according to suffix/prefix. useful if you want to search for none matches. Defaults to False.
            renameright (bool, optional): Rename all column from right. Defaults to False.

        Returns:
            list: returns the new table, each row has deepcopy of values
        """
        left = self.data
        right = other.data
        # Identifera nycklar som behöver namnbyten
        keys2 = {}
        for key in right[0].keys():
            if key not in on or keepkeys:
                name = key
                if key in left[0] or renameright:
                    if prefix:
                        name = prefix + "_" + name
                    else:
                        name += "_" + suffix
                keys2[key] = name
            pass
        # join
        result = []
        for l in left:
            have_match = False
            inner = None
            for r in right:
                flag = True
                for o in on:
                    if l[o] != r[o]:
                        flag = False
                        break
                have_match = have_match or flag
                if flag:
                    inner = copy.deepcopy(l)
                    for key, val in r.items():
                        if key in on and (not keepkeys):
                            continue
                        k2 = keys2[key]
                        inner[k2] = copy.deepcopy(val)
                    result.append(inner)
                pass
            # om left join så behöver vi hantera fallet när inga matchningar finns
            if type=="left" and not have_match:
                inner = copy.deepcopy(l)
                for orig_key, new_key in keys2.items():
                    inner[new_key] = None
                result.append(inner)
            pass
        return Tod(result)

    def clone(self):
        return copy.deepcopy(self)

    def nvalue(value1, value2=0):
        if value1 is None:
            return value2
        else:
            return value1

    def __init__(self, table, deepcopy=False):
        if deepcopy:
            self.data = copy.deepcopy(table)
        else:
            self.data = table

    def sort(self):
        self.data.sort(key=lambda x: tuple([ (y is not None, y) for x,y in x.items()]) )    

    def filter(self, condition, deepcopy=False):
        data = [ x for x in self.data if condition(x) ]
        if deepcopy:
            data = copy.deepcopy(data)
        return Tod(data)
        
    def __getitem__(self, key):
        if isinstance(key, tuple):
            return self.data[key[0]][key[1]]
        elif isinstance(key, int):
            return self.data[key]
        else:
            raise ValueError("Unsuported key type type="+type(key))
    def select(self,*column):
        t = []
        for row in self.data:
            d = {}
            for c in column:
                d[c] = copy.deepcopy(row[c])
            t.append(d)
        return Tod(t)

    def rename_column(self, names:dict):
        for i,row in enumerate(self.data):
            t = {}
            for column_name, column_value in row.items():
                if column_name in names:
                    new_name = names[column_name]
                    t[new_name] = copy.deepcopy(column_value)
                else:
                    t[column_name] = copy.deepcopy(column_value)
            self.data[i]=t

    def distinct_column_values(self, *column, ascending=None, keepnone=False):
        result = {x:set() for x in column}
        for row in self.data:
            for c in column:
                v = row[c]
                if v is None and not keepnone:
                    continue
                result[c].add(v)
        if len(column) == 1:
            result = result[column[0]]
            if ascending is not None:
                result=sorted(result,key=lambda x: (x is None, x), reverse=(not ascending))
            return result[column[0]]
        else:
            if ascending is not None:
                for key,val in result.items():
                    result[key] = sorted(list(val), key=lambda x: (x is not None, x),reverse=(not ascending))
            return result

    def fix_columns(self):
        column_names = set()
        for row in self.data:
            for k in row.keys():
                column_names.add(k)
        column_names = sorted(column_names)
        for i in range(len(self.data)):
            new = {}
            old = self.data[i]
            for c in column_names:
                if c in old:
                    new[c] = old[c]
                else:
                    new[c] = None
            self.data[i] = new

    def column_max(self, column_name:str, nonevalue:any) -> any:
        t:any = Tod.nvalue(self.data[0][column_name], nonevalue)
        for row in self.data:
            value:any = Tod.nvalue(row[column_name] ,nonevalue)
            if value > t:
                t = value
        
        return t

    def column_min(self, column_name:str, nonevalue:any) -> any:
        t:any = Tod.nvalue(self.data[0][column_name], nonevalue)
        for row in self.data:
            value:any = Tod.nvalue(row[column_name], nonevalue)
            if value < t:
                t = value
        return t
    def set_column(self, column:str, value:any):
        for row in self.data:
            row[column] = value

    def column_sum(self, column_name:str) -> float:
        """Sum values in column, skips "None"
        Args:
            table (list): Table to search
            column_name (str): Name of column to sum.

        Returns:
            float: summed value
        """
        sum:float = 0
        for row in self.data:
            value:float = row[column_name]
            if value is not None:
                sum += value
        return sum

    def column_avg(self, column_name, count_none=True):
        sum = 0
        count = len(self.data)
        for row in self.data:
            value = row[column_name]
            if value is not None:
                sum += value
            elif not count_none:
                count -= 1
        if count == 0:
            return None
        else:
            return sum/count

    def column_count(self, column_name:str=None, condition=None, filter=None) -> tuple:
        """
        If column_name is used, the lambda in condition and filter gets the column_name
        as argument. If no column_name is used, the lambdas get row as argument.
        That is, only use column_name for simple counts

        Args:
            table (list): _description_
            column_name (str, optional): _description_. Defaults to None.
            condition (_type_, optional): _description_. Defaults to None.
            filter (_type_, optional): _description_. Defaults to None.

        Returns:
            tuple: _description_
        """
        table = self.data
        count_true =  0
        count_false = 0
        if condition is None:
            condition = lambda x: False if x is None else True
        if filter is None:
            filter = lambda x: True
        for row in table:
            test = False
            if column_name is not None:
                if not filter(row[column_name]):
                    # skip row
                    continue
                test = condition(row[column_name])
            else:
                if not filter(row):
                    # skip row
                    continue
                test =  condition(row)

            if test:
                count_true += 1
            else:
                count_false += 1
        return (count_true, count_false)                    

        
class TodGroup:
    data:Tod  
    __keylen:int

    __lambda_rank_sort = lambda x,orderby: tuple([ 
        ( 
            (
                (v is not None) if orderby[k] else (v is None)
            ),
            (v if orderby[k] else (v if v is None else -v))
        )
        for k,v in x.items() if k in orderby
    ])

    def clonedata(self):
        self.data = copy.deepcopy(self.data)


    def row_number(self, orderby, rankname="rank"):
        t = self.data
        for key,tab in t.items():
            tab.sort(
                key=lambda x: TodGroup.__lambda_rank_sort(x,orderby)
            ) 
            for i in range(0,len(tab)):
                tab[i][rankname] = i+1

    def dense_rank(self, orderby, rankname="rank"):
        """orderby is a dict, key is column_name and boolean as value
        True = Ascending, False=Descending
        Only supports Descending numbers, crash on everything else
        that cant be negative. Probably solvable by using a comparator


        Args:
            table (_type_): _description_
            by (_type_): _description_
            orderby (_type_): _description_
            rankname (str, optional): _description_. Defaults to "rank".
        """
        t = self.data
        for key,tab in t.items():
            tab.sort(
                key=lambda x: TodGroup.__lambda_rank_sort(x,orderby)
            ) 
            lval = { k:tab[0][k] for k in orderby }
            rank = 1
            tab[0][rankname] = rank
            for i in range(1,len(tab)):
                val = { k:tab[i][k] for k in orderby }
                if lval != val:
                    rank += 1
                    lval = val
                tab[i][rankname] = rank
            pass
        pass            

    def __init__(self, tod, *by, deepcopy=False):
        self.__keylen = len(by)*2
        if isinstance(tod, list):
            tod = Tod(list)
        d = {}
        for row in tod.data:
            #t = tuple([ (x,row[x]) for x in by])
            t = tuple([e for tupl in [(x,row[x]) for x in by] for e in tupl])
            if (t not in d):
                d[t] = [row]
            else:
                d[t].append(row)
        self.data = d
        if deepcopy:
            self.data = copy.deepcopy(self.data)

    def __getitem__(self, key):
        if len(key) != self.__keylen:
            raise ValueError("Given key is of length " + str(len(key)) + ", but group key has length=" + str(self.__keylen))
        key=tuple(key)
        return self.data[key]

    def to_tod(self):
        d = []
        for key, data in self.data.items():
            d += data
        return Tod(d)


if __name__ == '__main__':

    table1 = [
        {"customer_id": 4, "name": "Four Inc" },
        {"customer_id": 1, "name": "One Inc" },
        {"customer_id": 3, "name": "Three Inc" },
        {"customer_id": 2, "name": "Two Inc" },
    ]
    table2 = [
        {"customer_id": 1, "sale": 100, "priority": 1},
        {"customer_id": 1, "sale": 150, "priority": None },
        {"customer_id": 1, "sale": 150, "priority": 2 },
        {"customer_id": 2, "sale": 20000, "priority": 1 },
        {"customer_id": 2, "sale": 1400, "priority": 1 },
        {"customer_id": 2, "sale": 2500, "priority": 1 },
        {"customer_id": 2, "sale": 60, "priority": 1 },
        {"customer_id": 3, "sale": 3400, "priority": 1 },
        {"customer_id": 3, "sale": None, "priority": 1 },
        {"customer_id": 3, "sale": None, "priority": 2 },
    ]
    tab = Tod(table2)
    #print(tab[1])
    t = tab.select("sale","priority")
    data = tab.data
    data2 = [x for x in data if Tod.nvalue(x['sale']) > 0] 
    gr = TodGroup(tab, "customer_id", deepcopy=True)
    gr.row_number(orderby={"sale": True}, rankname="rank1")
    gr.row_number(orderby={"sale": False}, rankname="rank2")
    gr.dense_rank(orderby={"sale": True}, rankname="rank3")
    gr.dense_rank(orderby={"sale": False}, rankname="rank4")
    #print(gr["customer_id",1,"sale",100])
    
    """
    for key,data in gr.data.items():
        print(key)
        print(json.dumps(data, indent=4))
        print("----------------------------------------")
    """
    t = gr.to_tod()
    for row in t.data:
        print(row)
    print("--orginal----------------------------------")
    #tab.rename_column({"priority": "prio", "sale": "elas"})
    x=tab.column_count("priority", condition=lambda x: Tod.nvalue(x) < 2, filter=lambda x: x is not None)
    print(x)
    x = tab.filter(lambda x: Tod.nvalue(x['sale']) < 1000 )
    #x = tab.column_max("sale", 0)
    #print(x)
    for row in tab.data:
        print(row)
    
    print("--unique----------------------------------")
    x.set_column('prio',242)
    
    for row in x.data:
        print(row)
    print("--XXX----------------------------------")        
    tab.fix_columns()
    for row in tab.data:
        print(row)
    print("--Inner join----------------------------------")        
    x=Tod(table1).join(other=Tod(table2), on=["customer_id"], type="left",keepkeys=True)
    for row in x.data:
        print(row)
    #u = t.distinct_column_values("rank4","rank1","customer_id", "sale", ascending=True, keepnone=True)
    #print(u)
    
    #print(t[0])