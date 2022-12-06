import sys, os, re
from .api import Api


class ParseDecoratedCppApis:
    """
    Parse a C++ header file and get the set of all decorated prototypes
    and autogen statements. This parser makes various assumptions about
    the C++ style. These are defined in:
        _is_class, _is_namespace, _is_api
    :return: self.api_map
             [path][namespace+class]['apis'][api_name] -> API()
             [path][namespace+class]['start'] -> autogen macro start
             [path][namespace+class]['end'] -> autogen macro end
             [path][namespace+class]['indent'] -> autogen macro indentation
    """

    def __init__(self, files, api_decs):
        """
        Load the C++ header file and clean out commented lines

        :param files: the files to augment
        :param api_decs: the API decorators to process
        """
        self.files = files
        self.api_decs = api_decs
        self.api_map = {}

    def _clean_lines(self, class_lines):
        """
        Removes all lines encapsulated between /* and */.
        Removes all excessive newlines
        Intended to remove classes which have been commented out.

        :param class_lines: the lines in the class file
        :return: a subset of the class_lines
        """
        # Remove newlines
        class_lines = [(i, line) for i, line in class_lines if len(line)]

        # Remove comments
        new_lines = []
        in_comment = False
        for i, line in class_lines:
            if self._is_comment_start(line):
                in_comment = True
                if self._is_comment_end(line):
                    in_comment = False
                continue
            if self._is_comment_end(line):
                in_comment = False
                continue
            if in_comment:
                continue
            new_lines.append((i, line))
        return new_lines

    def _is_comment_start(self, line):
        """
        Determines if a line begins with /* (excluding whitespace)

        :param line: the line of the C++ file to check
        :return: True or False
        """

        return '/*' == line.strip()[0:2]

    def _is_comment_end(self, line):
        """
        Determines if a line ends with */ (excluding whitespace)

        :param line: the line of the C++ file to check
        :return: True or False
        """

        return '*/' == line.strip()[-2:]

    def parse(self):
        self._parse_files()
        self._apply_decorators()

    def _parse_files(self):
        for path in self.files:
            self.hpp_file = path
            with open(self.hpp_file) as fp:
                self.orig_class_lines = fp.read().splitlines()
                class_lines = list(enumerate(self.orig_class_lines))
                self.class_lines = self._clean_lines(class_lines)
                self.only_class_lines = list(zip(*self.class_lines))[1]
            self._parse()

    def _apply_decorators(self):
        while self._has_unmodified_apis():
            for api_dec in self.api_decs:
                api_dec.modify(self.api_map)
                self._strip_decorator(api_dec)

    def _strip_decorator(self, api_dec):
        for path, namespace_dict in self.api_map.items():
            for namespace, api_dict in namespace_dict.items():
                for api in api_dict['apis'].values():
                    api.decorators.remove(api_dec.dec)
                    if len(api.decorators) == 0:
                        del api_dict['apis'][api.name]

    def _has_unmodified_apis(self):
        for path, namespace_dict in self.api_map.items():
            for namespace, api_dict in namespace_dict.items():
                if len(api_dict['apis']):
                    return True
        return False

    def _parse(self, namespace=None, start=None, end=None):
        if start == None:
            start = 0
        if end == None:
            end = len(self.class_lines)
        i = start
        while i < end:
            if self._is_namespace(i) or self._is_class(i):
                i = self._parse_class_or_ns(namespace, i)
                continue
            elif self._is_api(i):
                i = self._parse_api(namespace, i)
                continue
            elif self._is_autogen(i):
                i = self._parse_autogen(namespace, i)
                continue
            i += 1
        return i

    def _parse_api(self, namespace, i):
        """
        Determines the set of text belonging to a particular API.
        ASSUMPTIONS:
        1. If templated, the prior line must end with a ">"
            VALID:
                template<typename T, typename S,
                         class Hash = std::hash<S>
                         >
                RPC void hello() {}
            INVALID:
                template<typename T> RPC void hello();
        2. The end of the argument list is always on the same line as
        either the terminating semicolon or curly brace
            VALID:
                RPC void hello() {
                }

                RPC void hello2();
            INVALID
                RPC void hello()
                {}

                RP

        :param namespace: the C++ namespace + class the API belongs to
        :param i: the line containing part of the API definition
        :return: None. Modifies the
        """

        tmpl_str = self._get_template_str(i)
        api_str,api_end = self._get_api_str(i)
        api = Api(api_str, tmpl_str)
        self._induct_namespace(namespace)
        self.api_map[self.hpp_file][namespace]['apis'][api.name] = api
        return api_end + 1

    def _get_template_str(self, i):
        """
        Starting at i, parses upwards until the template keyword is reached.

        :param i: The start of the function API
        :return: The template text
        """

        if i == 0:
            return None
        if '>' not in self.only_class_lines[i-1]:
            return None
        tmpl_i = None
        for class_line in reversed(self.only_class_lines[:i-1]):
            toks = class_line.split()
            if 'template' in toks[0]:
                tmpl_i = i
                break
            i -= 1
        if tmpl_i is None:
            return None
        tmpl_str = self.only_class_lines[tmpl_i:i]
        return tmpl_str

    def _get_api_str(self, i):
        """
        Gets the set of text encompassing the entire API string

        :param i: the first line of the API
        :return: the API text
        """

        api_end = i
        for line in self.only_class_lines[i:]:
            if ');' in line:
                break
            if ':' in line:
                break
            if ') {' in line:
                break
            api_end += 1
        api_str = "\n".join(self.only_class_lines[i:api_end+1])
        return api_str, api_end

    def _parse_class_or_ns(self, namespace, i):
        """
        Parse all APIs within the boundaries of a class or namespace.

        ASSUMPTIONS:
        1. A class is terminated with the first }; that has equivalent
        indentation to the class declaration
            VALID:
                class hello {
                };  // "class" and }; are aligned vertically
            INVALID:
                class hello {
                  };  // "class" and }; are misaligned vertically

        :param namespace: the current namespace the class is apart of
        :param i: The line to start parsing at
        :return: The index of the terminating };
        """

        # Get the class name + indentation
        line = self.class_lines[i][1]
        toks = re.split("[ :]", line)
        toks = [tok .strip() for tok in toks if tok is not None and len(tok)]
        class_name = toks[1]
        indent = self._indent(line)
        # Find the end of the class (};)
        end = i
        end_of_class = f"{indent}}};"
        for off, line in enumerate(self.class_lines[i+1:]):
            if end_of_class == line[0:len(end_of_class)]:
                end += off
                break
        # Parse all lines for prototypes
        namespace = self._ns_append(namespace, class_name)
        return self._parse(namespace, i+1, end)

    def _parse_autogen(self, namespace, i):
        """
        Parse the AUTOGEN keyword statements for a particular namespace

        :param namespace: the current namespace the autogen is apart of
        :param i: The line to start parsing at
        :return:
        """

        self._induct_namespace(namespace)
        true_i, line = self.class_lines[i]
        line = line.strip()
        if line == self.autogen_dec_start:
            self.api_map[self.hpp_file][namespace]['start'] = true_i
            self.api_map[self.hpp_file][namespace]['indent'] = \
                self._indent(line)
        else:
            self.api_map[self.hpp_file][namespace]['end'] = true_i
        return i + 1

    def _is_namespace(self, i):
        """
        Determine whether a tokenized line defines a C++ namespace.

        ASSUMPTIONS:
        1. A namespace is defined entirely on a single line
            VALID:
                namespace hello {
            INVALID:
                namespace hello
                {

        :param i: The line to start parsing at
        :return: True or False
        """
        line = self.only_class_lines[i]
        toks = line.split()
        # "namespace hello {" (is 3 tokens)
        if len(toks) < 3:
            return False
        if toks[0] != 'namespace':
            return False
        if toks[2] != '{':
            return False

    def _is_class(self, i):
        """
        Determine whether a tokenized line defines a C++ class, struct,
        or union definition. Ignores class declarations (e.g., class hello;).

        ASSUMPTIONS:
        1. Class definitions do not contain any ';', which may be possible
        in very boundary-case circumstances.
            VALID:
                class hello {
                class hello : public language {
                class hello : public language<int> {
                class hello : public language<MACRO,a> {
                class hello:
                    public language
                    {
            INVALID:
                class hello : public language<MACRO(a, ;)> {

        :param i: The line to start parsing at
        :return: True or False
        """

        line = self.only_class_lines[i]
        toks = line.split()
        if len(toks) < 2:
            return False
        if toks[0] != 'class' and toks[0] != 'struct' and toks[0] != 'union':
            return False
        for true_i, line in self.class_lines[i:]:
            toks = line.split()
            if toks[-1] == ';':
                return False
            if toks[-1] == '{':
                return True
        raise f"class {toks[1]} definition missing either ; or {{"

    def _is_api(self, i):
        """
        Determine whether a line defines a class method or function
        ASSUMPTIONS:
        1. The function is marked with an API decorator

        :param i: The line to start parsing at
        :return: True or False
        """

        line = self.only_class_lines[i]
        toks = line.split()
        # Determine if the line has a decorator
        for api_dec in self.api_decs:
            for tok in toks:
                if tok == api_dec.api_dec:
                    return True
        return False

    def _is_autogen(self, i):
        """
        Determine whether a line encompasses the autogen headers

        :param i: The line to start parsing at
        :return: True or False
        """

        line = self.only_class_lines[i].strip()
        for api_dec in self.api_decs:
            if line == api_dec.autogen_dec_start:
                return True
            if line == api_dec.autogen_dec_end:
                return True
        return False

    def _induct_namespace(self, namespace):
        if self.hpp_file not in self.api_map:
            self.api_map[self.hpp_file] = {}
        if namespace not in self.api_map[self.hpp_file]:
            self.api_map[self.hpp_file][namespace] = {
                'apis': {},
                'start': None,
                'end': None,
                'indent': None
            }

    @staticmethod
    def _ns_append(namespace, suffix):
        if namespace is None or len(namespace) == 0:
            return suffix
        return namespace + suffix

    @staticmethod
    def _indent(line):
        ilen = len(line) - len(line.lstrip())
        return line[0:ilen]