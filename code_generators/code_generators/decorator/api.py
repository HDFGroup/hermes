import sys, os, re
from enum import Enum


class Api:
    """
    Parses a C++ function prototype to determine:
    1. The arguments to the function
    2. The return value of the function
    3. The function decorators
    4. The function template
    """

    def __init__(self, path, namespace, api_str, all_decorators,
                 template_str=None, doc_str=None):
        self.path = path  # The file containing the API
        self.namespace = namespace  # The namespace of the API
        self.api_str = api_str  # Original C++ API string
        self.all_decorators = all_decorators
        if self.all_decorators is None:
            self.all_decorators = []
        self.template_str = template_str  # Original C++ API template string
        self.doc_str = doc_str
        self.name = None  # The name of the API
        self.ret = None  # Return value of the API
        self.params = []  # The variables in the API
        self.decorators = []  # The set of function decorators
        self._decompose_prototype(api_str)

    def _decompose_prototype(self, api_str):
        """
        Parses a C++ api string

        [DEC1] [DEC2] ... [RETURN_TYPE] [API_NAME](
            T1 param1,
            T2 param2 = R2(T3, T4),
            T3 param3 = R3()
        );

        :param api_str: the C++ api to parse
        :return: None. But modifies the name, type, parameters,
        and decorators of this class
        """

        # Remove all spaces around operators
        toks = self.split_cpp_toks(api_str)

        # Remove all strings and comments
        toks = self._remove_strings_and_comments(toks)

        # Parse the API
        self._parse(0, toks)

        # Check the special

    def get_args(self):
        """
        Create string to forward the parameters
        passed to this API to another API

        Example:
            Hello(int a, int b, int c) {
              Hello2(a, b, c);
            }
            "int a, int b, int c" would be the return value of get_args

        :return: Typed argument list
        """

        if len(self.params) == 0:
            return ""
        args = [f"{arg[0]} {arg[1]}" for arg in self.params]
        return ", ".join(args)

    def pass_args(self):
        """
        Create string to forward the parameters
        passed to this API to another API

        Example:
            Hello(int a, int b, int c) {
              Hello2(a, b, c);
            }
            "a, b, c" is the return value of pass_args

        :return: Untyped argument list
        """

        if self.params is None:
            return ""
        args = [arg[1] for arg in self.params if arg[0] != '']
        return ", ".join(args)

    def get_decorator_macros(self, exclude=None):
        if exclude is None:
            exclude = []
        return [macro for macro in self.decorators
                if macro not in exclude]

    def get_decorator_macro_str(self, exclude=None):
        return " ".join(self.get_decorator_macros(exclude))

    def rm_decorator_by_macro(self, exclude_macro):
        self.decorators = [macro for macro in self.decorators
                           if macro != exclude_macro]

    """
    TOKENIZATION
    """

    @staticmethod
    def _remove_strings_and_comments(toks):
        i = 0
        new_toks = []
        while i < len(toks):
            if Api._is_ml_comment_start(i, toks):
                i = Api._end_of_ml_comment(i + 1, toks)
                continue
            elif Api._is_sl_comment_start(i, toks):
                i = Api._end_of_sl_comment(i + 1, toks)
                continue
            elif Api._is_string(i, toks):
                i = Api._end_of_string(i + 1, toks)
                new_toks.append("PLACE")
                continue
            elif Api._is_whitespace(i, toks):
                i += 1
                continue
            new_toks.append(toks[i])
            i += 1
        return new_toks

    @staticmethod
    def _end_of_string(i, toks):
        while i < len(toks):
            if Api._is_string(i, toks):
                return i + 1
            i += 1

    @staticmethod
    def _end_of_ml_comment(i, toks):
        while i < len(toks):
            if Api._is_ml_comment_end(i, toks):
                return i + 2
            i += 1

    @staticmethod
    def _end_of_sl_comment(i, toks):
        while i < len(toks):
            if Api._is_sl_comment_end(i, toks):
                return i + 1
            i += 1

    @staticmethod
    def _is_ml_comment_start(i, toks):
        if i + 1 >= len(toks):
            return False
        return toks[i] == '/' and toks[i + 1] == '*'

    @staticmethod
    def _is_ml_comment_end(i, toks):
        if i + 1 >= len(toks):
            return False
        return toks[i] == '/' and toks[i + 1] == '*'

    @staticmethod
    def _is_sl_comment_start(i, toks):
        if i + 1 >= len(toks):
            return False
        return toks[i] == '/' and toks[i + 1] == '/'

    @staticmethod
    def _is_sl_comment_end(i, toks):
        return '\n' in toks[i]

    @staticmethod
    def _is_string(i, toks):
        is_str = toks[i] == '\"'
        if i == 0:
            return is_str
        if toks[i - 1] == '\\':
            return False
        return is_str

    @staticmethod
    def _is_whitespace(i, toks):
        return re.match("\s+", toks[i])

    @staticmethod
    def split_cpp_toks(text):
        chars = ['\\\'', '\\\"', '\(', '\)', '<', '>', '\[', '\]', '\{', '\}',
                 '\+', '\-', '=', '\*', '/', '\|', '\*', '&', ',', '\:', ';']
        chars = "".join(chars)
        chars = f"[{chars}]"
        text = " ".join(text.split())
        toks = re.split(f"({chars}|\s+)", text)
        toks = Api._clean(toks)
        return toks

    @staticmethod
    def _clean(toks):
        toks = [tok for tok in toks if tok is not None and len(tok) > 0]
        return toks

    @staticmethod
    def _is_valid_name(tok):
        return re.match('[a-zA-z0-9_]+', tok)


    """
    PARSING
    """

    def _parse(self, i, toks):
        fn_name_i = self._find_function_name(i, toks)
        self.name = toks[fn_name_i]
        self._parse_decorators_and_type(i, fn_name_i, toks)
        self.params = self._parse_params(fn_name_i + 2, toks)

    def _find_function_name(self, i, toks):
        while i < len(toks):
            if toks[i] == '(':
                return i-1
            if toks[i] == '<':
                i = self._parse_brackets('<', '>', i, toks)
                continue
            if toks[i] == '(':
                i = self._parse_brackets('(', ')', i, toks)
                continue
            if toks[i] == '[':
                i = self._parse_brackets('[', ']', i, toks)
                continue
            if toks[i] == '{':
                i = self._parse_brackets('{', '}', i, toks)
                continue
            i += 1
        return i

    def _parse_decorators_and_type(self, i, fn_name_i, toks):
        type_toks = []
        while i < fn_name_i:
            if self._is_decorator(i, toks):
                self.decorators.append(toks[i])
            else:
                type_toks.append(toks[i])
            i += 1
        self.ret = self._merge_type_tuple(type_toks)

    def _parse_params(self, i, toks):
        params = []
        do_parse_param = True
        while do_parse_param:
            i, param_info, do_parse_param = self._parse_param(i, toks)
            if param_info == None:
                continue
            params.append(param_info)
        return params

    def _parse_param(self, i, toks):
        param_start = i
        if i >= len(toks) or toks[i] == ')':
            return i + 1, None, False
        while i < len(toks):
            tok = toks[i]
            if tok == ',':
                param_name = toks[i - 1]
                param_type = self._merge_type_tuple(toks[param_start:i - 1])
                param_val = None
                return i + 1, (param_type, param_name, param_val), True
            if tok == ')':
                param_name = toks[i - 1]
                param_type = self._merge_type_tuple(toks[param_start:i - 1])
                param_val = None
                return i + 1, (param_type, param_name, param_val), False
            if tok == '=':
                param_name = toks[i - 1]
                param_type = self._merge_type_tuple(toks[param_start:i - 1])
                i, param_val = self._parse_param_val(i + 1, toks)
                param_val = self._merge_type_tuple(param_val)
                return i, (param_type, param_name, param_val), True
            if self._is_bracket(i, toks):
                i = self._parse_all_brackets(i, toks)
                continue
            i += 1
        return i + 1, None, False

    def _parse_param_val(self, i, toks):
        param_val_start = i
        while i < len(toks):
            tok = toks[i]
            if tok == ',':
                return i + 1, toks[param_val_start:i]
            if tok == ')':
                return i + 1, toks[param_val_start:i]
            if self._is_bracket(i, toks):
                i = self._parse_all_brackets(i, toks)
                continue
            i += 1
        return i

    def _is_bracket(self, i, toks):
        brackets = ['<', '(', '[', '{']
        return toks[i] in brackets

    def _parse_all_brackets(self, i, toks):
        tok = toks[i]
        if tok == '<':
            return self._parse_brackets('<', '>', i, toks)
        if tok == '(':
            return self._parse_brackets('(', ')', i, toks)
        if tok == '[':
            return self._parse_brackets('[', ']', i, toks)
        if tok == '{':
            return self._parse_brackets('{', '}', i, toks)

    def _parse_brackets(self, left, right, i, toks):
        depth = 0
        while i < len(toks):
            tok = toks[i]
            if tok == left:
                depth += 1
            if tok == right:
                depth -= 1
            if depth == 0:
                return i+1
            i += 1
        return i

    def _is_decorator(self, i, toks):
        for api_dec in self.all_decorators:
            if toks[i] == api_dec.macro:
                return True
            return False

    def _merge_type_tuple(self, type_tuple):
        new_toks = []
        i = 0
        while i < len(type_tuple):
            tok = type_tuple[i]
            new_toks.append(tok)
            if i + 1 < len(type_tuple):
                this_valid = self._is_valid_name(type_tuple[i])
                next_valid = self._is_valid_name(type_tuple[i + 1])
                if this_valid and next_valid:
                    new_toks.append(' ')
            i += 1
        return "".join(new_toks)

    """
    HELPERS
    """

    def __str__(self):
        strs = [
            f"PATH: {self.path}",
            f"NAMESPACE: {self.namespace}",
            f"DECORATORS: {self.get_decorator_macros()}",
            f"RETURN: {self.ret}",
            f"NAME: {self.name}",
            f"PARAMS: {self.params}",
        ]
        return "\n".join(strs)

    def __repr__(self):
        strs = [
            f"PATH: {self.path}",
            f"NAMESPACE: {self.namespace}",
            f"DECORATORS: {self.get_decorator_macros()}",
            f"RETURN: {self.ret}",
            f"NAME: {self.name}",
            f"PARAMS: {self.params}",
        ]
        return "\n".join(strs)