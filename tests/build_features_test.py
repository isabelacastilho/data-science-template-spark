import unittest
import pandas as pd
from src.build_features import *


df = pd.DataFrame({'A': pd.Categorical(["a", "b", "a", "a"]),
                   'B': pd.Categorical(["test", "train", "test", "train"]),
                   'C': pd.Categorical(["test", "train", "test", "train"]),
                   'D': pd.Categorical(["test", "train", "test", "train"])})


class TestBuildFeatures(unittest.TestCase):
    def test_replace_value(self):
        input_df = pd.DataFrame({'A': pd.Categorical(['a', 'b', 'a', 'd']),
                                 'B': pd.Categorical(["b", "b", "a", "a"])})

        expected = {'A': {0: 'e', 1: 'c', 2: 'e', 3: 'd'},
                    'B': {0: 'b', 1: 'b', 2: 'a', 3: 'a'}}

        result = replace_values(input_df, 'A', {'b': 'c', 'a': 'e'})

        self.assertDictEqual(result.to_dict(), expected)

    def test_check_for_similar_values(self):
        input_df = pd.DataFrame({'A': pd.Categorical(["cradiff", "cardiff", "bristol", "pardiff", "cardif"]),
                                 'B': pd.Categorical(["b", "b", "a", "a", "a"])})

        expected = [['cradiff', 'cardiff'], ['cardiff', 'pardiff'], ['cardiff', 'cardif']]

        result = check_for_similar_values(input_df, 'A')
        words = [[i[0], i[1]] for i in result]

        self.assertListEqual(words, expected)

    def test_find_unacceptable_values_numerical(self):
        input_df = pd.DataFrame({'A': pd.Categorical(["a", "b", "c", "d", "e"]),
                              'B': [1, 2, 5, 7, 8]})

        expected = {'A': {2: 'c', 3: 'd', 4: 'e'},
                    'B': {2: 5, 3: 7, 4: 8}}

        result = find_unacceptable_values(input_df, 'B', '>=5')

        self.assertDictEqual(result.to_dict(), expected)

    def test_find_unacceptable_values_categorical(self):
        input_df = pd.DataFrame({'A': pd.Categorical(["a", "b", "c", "c", "e"]),
                                 'B': [1, 2, 5, 7, 8]})

        expected = {'A': {0: 'a', 2: 'c', 3: 'c'},
                    'B': {0: 1, 2: 5, 3: 7}}

        result = find_unacceptable_values(input_df, 'A', ['b', 'e'])

        self.assertDictEqual(result.to_dict(), expected)

    def test_simplify_text(self):
        input_df = pd.DataFrame({'A': pd.Categorical(["dsfdv, fve(dvw)!", "Ecbgf %df", "EdFg"]),
                                 'B': [1, 2, 5]})
        expected = {'A': {0: 'dsfdv fvedvw', 1: 'ecbgf df', 2: 'edfg'},
                    'B': {0: 1, 1: 2, 2: 5}}

        result = simplify_text(input_df, 'A')

        self.assertDictEqual(result.to_dict(), expected)

    def test_simplify_text_tokenize(self):
        input_df = pd.DataFrame({'A': pd.Categorical(["dsfdv, fve(dvw)!", "Ecbgf %df", "EdFg"]),
                                 'B': [1, 2, 5]})
        expected = {'A': {0: ['dsfdv', 'fvedvw'], 1: ['ecbgf', 'df'], 2: ['edfg']},
                    'B': {0: 1, 1: 2, 2: 5}}

        result = simplify_text(input_df, 'A', tokenize=True)

        self.assertDictEqual(result.to_dict(), expected)


if __name__ == '__main__':
    unittest.main()