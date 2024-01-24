# %% utility tests
def describe_table(table_name, table, file_path):
    """Print summary information about imported tables for sense checking."""
    print('\n')
    print("Loaded {0} with {1[0]} rows, {1[1]} columns from '{2}'".format(table_name, table.shape, file_path))
    print(table.head(3))
    return None


def change_in_count(variable_name, count_name, pre_count, post_count):
    """Print the change in count for a given value after preprocessing.
    
    This alerts you to whether the count of a variable has unintendedly 
    changed after preprocessing / imputation has taken place.
    """
    ratio = post_count/pre_count
    print("Modifying {0}: {1} changed {2:g} -> {3:g} (x{4:.2f})".format(variable_name,
                                                                        count_name,
                                                                        pre_count,
                                                                        post_count,
                                                                        ratio))
    return None
