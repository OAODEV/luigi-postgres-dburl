import re


class MultiReplacer(object):
    """
    Object for one-pass replace of multiple words

    Substituted parts will not be matched against other replace patterns, as opposed to when using multipass replace.
    The order of the items in the replace_pairs input will dictate replacement precedence.

    Constructor arguments:
    replace_pairs -- list of 2-tuples which hold strings to be replaced and replace string

    Usage:

    .. code-block:: python

        >>> replace_pairs = [("a", "b"), ("b", "c")]
        >>> MultiReplacer(replace_pairs)("abcd")
        'bccd'
        >>> replace_pairs = [("ab", "x"), ("a", "x")]
        >>> MultiReplacer(replace_pairs)("ab")
        'x'
        >>> replace_pairs.reverse()
        >>> MultiReplacer(replace_pairs)("ab")
        'xb'
    """

    def __init__(self, replace_pairs):
        """
        Initializes a MultiReplacer instance.

        :param replace_pairs: list of 2-tuples which hold strings to be replaced and replace string.
        :type replace_pairs: tuple
        """
        replace_list = list(replace_pairs)  # make a copy in case input is iterable
        self._replace_dict = dict(replace_list)
        pattern = '|'.join(re.escape(x) for x, y in replace_list)
        self._search_re = re.compile(pattern)

    def _replacer(self, match_object):
        # this method is used as the replace function in the re.sub below
        return self._replace_dict[match_object.group()]

    def __call__(self, search_string):
        # using function replacing for a per-result replace
        return self._search_re.sub(self._replacer, search_string)
