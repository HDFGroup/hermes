import sys, os, re

class Api:
    """
    Parses a C++ function prototype to determine:
    1. The arguments to the function
    2. The return value of the function
    3. The function decorators
    4. The function template
    """

    def __init__(self, api_str, template_str=None):
        self.api_str = api_str # Original C++ API string
        self.template_str = template_str # Original C++ API template string
        self.name = None  # The name of the API
        self.ret = None  # Return value of the API
        self.var_defs = None  # The variables in the API
        self.decorators = None # The set of function decorators
        self._decompose_prototype(api_str)

    def _clean(self, toks):
        return [tok for tok in toks if tok is not None and len(tok) > 0]

    def _decompose_prototype(self, api_str):
        """
        Parses a C++ api string

        :param api_str: the C++ api to parse
        :return: None. But modifies the name, type, parameters,
        and decorators of this class
        """

        # Split by parenthesis and semicolon
        toks = self._clean(re.split("[()]|;", api_str))
        proto, args = toks[0], toks[1]

        try:
            proto = self._clean(re.split("[ ]|(\*+)", proto))
            self.name = proto[-1]
            self.ret = " ".join(proto[:-1])
        except:
            print(f"Failed to decompose proto name: {proto}")
            exit()

        try:
            self.var_defs = []
            args = args.split(',')
            for arg in args:
                self.var_defs.append(self._get_arg_tuple(arg))
        except:
            print(f"Failed to decompose proto args: {args}")
            exit(1)

    def _get_arg_tuple(self, arg):
        """
        Parse a function argument into a type and name.
        E.g., int hi -> (int, hi)

        :param arg: The C++ text representing a function argument
        :return: (type, name)
        """

        arg_toks = self._clean(re.split("[ ]|(\*+)|(&+)", arg))
        if len(arg_toks) == 1:
            if arg_toks[0] == '...':
                type = ""
                name = "..."
                return (type, name)
        type = " ".join(arg_toks[:-1])
        type = type.replace(" ", "")
        type = type.replace("\n", "")
        name = arg_toks[-1]
        name = name.replace(" ", "")
        return (type, name)

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

        if len(self.var_defs) == 0:
            return ""
        try:
            args = [" ".join(arg_tuple) for arg_tuple in self.var_defs]
        except:
            print(f"Failed to get arg list: {self.var_defs}")
            exit(1)
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

        if self.var_defs is None:
            return ""
        args = [arg[-1] for arg in self.var_defs if arg[0] != '']
        return ", ".join(args)

class ParseDecoratedCppApis:
    """
    Parse a C++ header file and get the set of all decorated prototypes
    and autogen statements. This parser makes various assumptions about
    the C++ style. These are defined in:
        _is_class, _is_namespace, _is_api
    :return: self.api_map
             [namespace+class]['apis'][api_name] -> API()
             [namespace+class]['start'] -> autogen macro start
             [namespace+class]['end'] -> autogen macro end
             [namespace+class]['indent'] -> autogen macro indentation
    """

    def __init__(self, hpp_file, api_dec=None, autogen_dec=None,
                 ignore_autogen_ns=False):
        """
        Load the C++ header file and clean out commented lines

        :param hpp_file: the path to the C++ header file to parse
        :param api_dec: the name of the API decorator to search for
        :param autogen_dec: the name of the code autogen statement
        to search for
        :param ignore_autogen_ns: whether or not to consider the
        namespace the autogen macros are apart of
        """
        self.hpp_file = hpp_file
        self.api_dec = api_dec
        self.autogen_dec = autogen_dec
        self.api_map = {}

        # Load class file lines, but remove excess newlines and comments
        with open(self.hpp_file) as fp:
            self.orig_class_lines = fp.read().splitlines()
            class_lines = list(enumerate(self.orig_class_lines))
            self.class_lines = self._clean_lines(class_lines)
            self.only_class_lines = list(zip(*self.class_lines))[1]

    def set_decorators(self, api_dec, autogen_dec, ignore_autogen_ns=False):
        self.api_dec = api_dec
        self.autogen_dec = autogen_dec
        self.autogen_dec_start = f"{self.autogen_dec}_START"
        self.autogen_dec_end = f"{self.autogen_dec}_END"
        self.ignore_autogen_ns = ignore_autogen_ns

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

    def parse(self, namespace=None, start=None, end=None):
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
            elif self._is_autogen(namespace, i):
                i = self._parse_autogen(namespace, i)
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
        self.api_map[namespace][api.name] = api
        return api_end

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
        toks = [tok for tok in toks if tok is not None and len(tok)]
        class_name = toks[1]
        indent = self._indent()
        # Find the end of the class (};)
        end = i
        end_of_class = f"{indent}}};"
        for off, line in enumerate(self.class_lines[i+1:]):
            if end_of_class == line[0:len(end_of_class)]:
                end += off
                break
        # Parse all lines for prototypes
        namespace = self._ns_append(namespace, class_name)
        return self.parse(namespace, i+1, end)

    def _parse_autogen(self, namespace, i):
        """
        Parse the AUTOGEN keyword statements for a particular namespace

        :param namespace: the current namespace the autogen is apart of
        :param i: The line to start parsing at
        :return:
        """

        if self.ignore_autogen_ns:
            namespace = None
        self._induct_namespace(namespace)
        true_i, line = self.class_lines[i]
        line = line.strip()
        if line == self.autogen_dec_start:
            self.api_map[namespace]['start'] = true_i
            self.api_map[namespace]['indent'] = self._indent(line)
        else:
            self.api_map[namespace]['end'] = true_i
        return line

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
        Determine whether a tokenized line defines a C++ class definition.
        Ignores class declarations (e.g., class hello;).

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
        if toks[0] != 'class':
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
        for tok in toks:
            if tok == self.api_dec:
                return True
        return False

    def _is_autogen(self, i):
        """
        Determine whether a line encompasses the autogen headers

        :param i: The line to start parsing at
        :return: True or False
        """

        line = self.only_class_lines[i].strip()
        if line == self.autogen_dec_start:
            return True
        if line == self.autogen_dec_end:
            return True
        return False

    def _induct_namespace(self, namespace):
        if namespace not in self.api_map:
            self.api_map[namespace] = {
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