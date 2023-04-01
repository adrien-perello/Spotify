###########################################################################


# Scientific libraries
import pandas as pd
import jenkspy


###########################################################################


def jenks_filter(lst, nb_class=3, cutoff=2, strict=False, sort_by="alphabetically"):
    """_summary_

    Args:
        lst (_type_): _description_
        nb_class (int, optional): _description_. Defaults to 3.
        cutoff (int, optional): _description_. Defaults to 2.
        strict (bool, optional): _description_. Defaults to False.
        sort_by (str, optional): _description_. Defaults to 'alphabetically'.

    Returns:
        _type_: _description_
    """
    # use the jenks natural breaks optimization (min nb_classes = 2)
    if len(lst) == 0:
        return lst

    if len(lst) <= 2:
        return sorted(lst)

    df = pd.Series(lst).value_counts()
    try:
        lim = jenkspy.jenks_breaks(df, nb_class=nb_class)[-cutoff]
    except ValueError as VE:
        lim = df.min()
        strict = True

    result = df[df > lim] if strict else df[df >= lim]

    if sort_by == "count":
        return result.index.to_list()
    elif sort_by == "alphabetically":
        return sorted(result.index.to_list())
    raise ValueError(
        f"sort_by should be either 'alphabetically' or 'count', not {sort_by}"
    )


# -------------------------------------------------------------------------


def contains_any(df, values):
    """Find all rows which contains at least one of the values

    Args:
        df (DataFrame): the DataFrame to be searched
        values (str | Number | Sequence): the values to be searching for

    Raises:
        ValueError: type in the DataFrame should be consistent
                    (not a mix of sequence and number for example)

    Returns:
        pd.Series(bool): mask of the rows that contain 'values'
    """
    # Mask filter based on values union
    if isinstance(values, (str, int, float)):
        values = {values}
    elif not isinstance(values, set):
        values = set(values)

    try:
        lst_instance = df.applymap(type) == list  # check if df contains lists
        #                                         # using applymap() for DataFrame

    except AttributeError:  # using apply() for Series
        lst_instance = df.apply(type) == list

    if lst_instance.any().any() and not lst_instance.all().any():
        print(df[lst_instance])
        raise ValueError(f"The dataframe contains a mix of list instance and other.")

    elif lst_instance.all().any():
        return ~df.map(values.isdisjoint)

    try:
        return df.isin(values).any(axis=1)
    except ValueError:
        return df.isin(values)


# -------------------------------------------------------------------------


def contains_all(df, values):
    """Find all rows which contains aall of the values

    Args:
        df (DataFrame): the DataFrame to be searched
        values (str | Number | Sequence): the values to be searching for

    Returns:
        pd.Series(bool): mask of the rows that contain 'values'
    """
    # Mask filter based on values intersection
    if isinstance(values, (str, int, float)):
        values = {values}
    elif not isinstance(values, set):
        values = set(values)
    return df.map(values.issubset)
