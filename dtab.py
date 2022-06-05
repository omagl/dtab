import copy
import operator



def drop_column(table:list, columns:any, inplace = True):
    if not isinstance(columns,list):
        columns = [columns]
    tab = []
    for i, row in enumerate(table):
        row = copy.deepcopy(row)
        for column_name in columns:
            row.pop(column_name)
        if inplace:
            table[i] = row
        else:
            tab.append(row)
    if inplace:
        return None
    else:
        return tab


def rename_column(table:list, names:dict, inplace:bool = True):
    tab = []
    for i,row in enumerate(table):
        t = {}
        for column_name, column_value in row.items():
            if column_name in names:
                new_name = names[column_name]
                t[new_name] = copy.deepcopy(column_value)
            else:
                t[column_name] = copy.deepcopy(column_value)
        if inplace:
            table[i]=t
        else:
            tab.append(t)
    if inplace:
        return None
    else:
        return tab
def join_tables(left:list, right:list, on:list, type:str='inner', suffix:str="1", prefix:str=None, keepkeys:bool=False, renameright:bool=False) ->  list:
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
    return result
    
def column_max(table:list, column_name:str) -> any:
    t:any = table[0][column_name]
    for row in table:
        value:any = row[column_name] 
        if value:
            if value > t:
                t = value
    return t

def column_min(table:list, column_name:str) -> any:
    t:any = table[0][column_name]
    for row in table:
        value:any = row[column_name] 
        if value:
            if value < t:
                t = value
    return t


def column_sum(table:list, column_name:str) -> float:
    """Sum values in column, skips "None"
    Args:
        table (list): Table to search
        column_name (str): Name of column to sum.

    Returns:
        float: summed value
    """
    sum:float = 0
    for row in table:
        value:float = row[column_name]
        if value is not None:
            sum += value
    return sum

def column_avg(table, column_name, count_none=True):
    sum = 0
    count = len(table)
    for row in table:
        value = row[column_name]
        if value is not None:
            sum += value
        elif not count_none:
            count -= 1
    if count == 0:
        return None
    else:
        return sum/count

def column_count(table:list, column_name:str=None, condition=None) -> tuple:
    count_true =  0
    count_false = 0
    if condition is None:
        condition = lambda x: False if x is None else True
    for row in table:
        test = False
        if column_name is not None:
            test = condition(row[column_name])
        else:
            test =  condition(row)

        if test:
            count_true += 1
        else:
            count_false += 1
    return (count_true, count_false)

def groupby_column(table, by):
    d = {}
    for row in table:
        b = str(row[by])
        if b not in d:
            d[b] = [copy.deepcopy(row)]
        else:
            d[b].append(copy.deepcopy(row))
    return d

def table_sort(table:list, inplace=True):
    if inplace:
        table.sort(key=lambda x: tuple([ (y is not None, y) for x,y in x.items()]) )    
        return None
    else:
        t = copy.deepcopy(table)
        t.sort(key=lambda x: tuple([ (y is not None, y) for x,y in x.items()]) )    
        return t


def disctinct_column_values(table, column_name, keepnone=False):
    array = {}

    if not isinstance(column_name, list):
        column_name = [column_name]

    for name in column_name:
        array[name] = []

    for row in table:
        for name in column_name:
            array[name].append(row[name])
    
    result = {}
    for name in column_name:
        arr = array[name]
        arr.sort(key=lambda x: (x is None,x))
        length = len(arr)
        index = 1
        last_value= arr[0]
        unique = []
        unique.append(last_value)
        while index < length:
            value = arr[index]
            if last_value != value:
                if value is None:
                    if keepnone:
                        unique.append(value)
                else:
                    unique.append(value)
                last_value = value
            index += 1
        result[name] = unique
    if len(column_name) == 1:
        return result[column_name[0]]
    else:
        return result


    
    length = len(array)
    index = 1
    last_value = array[0]
    unique = []
    unique.append(last_value)
    
    while index < length:
        value = array[index]
        if last_value != value:
            if value is None:
                if keepnone:
                    unique.append(value)
            else:
                unique.append(value)
            last_value = value
        index += 1
    return unique


if __name__ == "__main__":
    def print_table(table):
        for row in table:
            print(row)

    print("Demo")
    print("----------------------------------")
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
    ]
    
    table_innerjoin =join_tables(left=table1, right=table2, on=['customer_id'])
    print("Inner join -----------------------")
    print_table(table_innerjoin)
    print("Renamed columns ------------------")
    rename_column(table_innerjoin, {"customer_id": "ID", "name": "COMPANY"}, inplace=True)
    print_table(table_innerjoin)
    print("Drop columns ---------------------")
    drop_column(table_innerjoin, ['sale','COMPANY'], inplace=True)
    print_table(table_innerjoin)


    table_leftjoin = join_tables(left=table1, right=table2, on=['customer_id'],type="left")
    print("Left join ------------------------")
    #table_leftjoin.sort(key=lambda x: (x['sale'] is None, x['sale'], x['priority'] is not None, x['priority']))
    print_table(table_leftjoin)
    table_sort(table_leftjoin)
    print("sorted ---------------------------")
    print_table(table_leftjoin)
    t = column_count(table_leftjoin, 'sale')
    print(t)
    nz = lambda x: 0 if x is None else x

    t = column_count(table_leftjoin, column_name='sale', condition=lambda x: True if nz(x)<2000 else False)
    print(f"{t} {t[0]/(t[0]+t[1])}")
    t = column_count(table_leftjoin, condition=lambda x: True if nz(x['sale'])>=100 and nz(x['sale']) <= 1400 else False)
    print(f"{t} {t[0]/(t[0]+t[1])}")
    t = column_count(table_leftjoin, condition=lambda x: True if nz(x['sale'])>=100 and nz(x['sale']) <= 1400 and x['priority'] is not None else False)
    print(f"{t} {t[0]/(t[0]+t[1])}")
    u = disctinct_column_values(table_leftjoin, ['customer_id','sale'])
    print(u)



    group_dict = groupby_column(table_leftjoin, 'customer_id')
    for customer_id, table in group_dict.items():
        sum_sales=column_sum(table, 'sale')
        nr_of_sales=column_count(table,'sale')[0]
        print(f"customer_id={customer_id}, sum_sales={sum_sales}, nr_of_sales={nr_of_sales}")


    
