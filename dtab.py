import copy

def dropcolumn(table:list, columns:any, inplace = False):
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


def rename(table:list, names:dict, inplace:bool = False):
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
def join(left:list, right:list, on:list, type:str='inner', suffix:str="1", prefix:str=None, keepkeys:bool=False, renameright:bool=False) ->  list:
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
def column_count(table, column_name):
    count = 0
    for row in table:
        if row[column_name] is not None:
            count +=1
    return count

def groupby(table, by):
    d = {}
    for row in table:
        b = str(row[by])
        if b not in d:
            d[b] = [copy.deepcopy(row)]
        else:
            d[b].append(copy.deepcopy(row))
    return d


if __name__ == "__main__":
    def print_table(table):
        for row in table:
            print(row)

    print("Demo")
    print("----------------------------------")
    table1 = [
        {"customer_id": 1, "name": "code inc" },
        {"customer_id": 2, "name": "python inc" },
        {"customer_id": 3, "name": "c++ inc" },
        {"customer_id": 4, "name": "c++ inc" },
    ]
    table2 = [
        {"customer_id": 1, "sale": 100, },
        {"customer_id": 1, "sale": 150 },
        {"customer_id": 2, "sale": 20000 },
        {"customer_id": 2, "sale": 1400 },
        {"customer_id": 2, "sale": 2500 },
        {"customer_id": 2, "sale": 60 },
        {"customer_id": 3, "sale": 3400 },
    ]
    
    table_innerjoin =join(left=table1, right=table2, on=['customer_id'])
    print("Inner join -----------------------")
    print_table(table_innerjoin)
    print("Renamed columns ------------------")
    rename(table_innerjoin, {"customer_id": "ID", "name": "COMPANY"}, inplace=True)
    print_table(table_innerjoin)
    print("Drop columns ---------------------")
    dropcolumn(table_innerjoin, ['sale','COMPANY'], inplace=True)
    print_table(table_innerjoin)


    table_leftjoin = join(left=table1, right=table2, on=['customer_id'],type="left")
    print("Left join ------------------------")
    print_table(table_leftjoin)

    group_dict = groupby(table_leftjoin, 'customer_id')
    for customer_id, table in group_dict.items():
        sum_sales=column_sum(table, 'sale')
        nr_of_sales=column_count(table,'sale')
        print(f"customer_id={customer_id}, sum_sales={sum_sales}, nr_of_sales={nr_of_sales}")

    
