import sys
import os
from abc import ABC, abstractmethod
import shlex
from tabulate import tabulate


class ArgParse(ABC):
    def __init__(self, args=None, exit_on_fail=True):
        if args is None:
            args = sys.argv[1:]
        elif isinstance(args, str):
            args = shlex.split(args)
        args = " ".join(args)
        self.binary_name = os.path.basename(sys.argv[0])
        self.orig_args = shlex.split(args)
        self.args = self.orig_args
        self.error = None
        self.exit_on_fail = exit_on_fail
        self.menus = []
        self.vars = {}
        self.remainder = None
        self.pos_required = True
        self.use_remainder = False

        self.menu = None
        self.define_options()
        self._parse()

    @abstractmethod
    def define_options(self):
        pass

    def add_menu(self, name=None, msg=None,
                 use_remainder=False):
        toks = []
        if name is not None:
            toks = name.split()
        self.menus.append({
            'name_str': " ".join(toks),
            'name': toks,
            'msg': msg,
            'num_required': 0,
            'pos_opts': [],
            'kw_opts': {},
            'use_remainder': use_remainder
        })
        self.pos_required = True
        self.menu = self.menus[-1]

    def start_required(self):
        self.pos_required = True

    def end_required(self):
        self.pos_required = False

    def add_arg(self,
                name,
                argtype=str,
                choices=None,
                default=None,
                msg=None,
                action=None,
                aliases=None):
        # Add all aliases
        if aliases is not None:
            for alias in aliases:
                if '-' in alias:
                    self.add_arg(alias, argtype, choices, default, msg, action)
                else:
                    raise f"Can't have a non-keyword alias: {alias}"
        # Handle the specific boolean argument case
        is_kwarg = '-' in name
        if is_kwarg and argtype == bool:
            self.add_bool_kw_arg(name, default, msg)
            return
        # Add general argument
        menu = self.menu
        arg = {
            'name': name,
            'dict_name': self._get_opt_name(name),
            'type': argtype,
            'choices': choices,
            'default': default,
            'action': action,
            'msg': msg,
            'required': self.pos_required,
            'has_input': True
        }
        if is_kwarg:
            self.pos_required = False
            menu['kw_opts'][name] = arg
        else:
            if self.pos_required:
                menu['num_required'] += 1
            menu['pos_opts'].append(arg)

    def add_bool_kw_arg(self,
                        name,
                        default,
                        msg=None,
                        is_other=False,
                        dict_name=None):
        menu = self.menu
        if dict_name is None:
            dict_name = self._get_opt_name(name, True)
        arg = {
            'name': name,
            'dict_name': dict_name,
            'type': bool,
            'choices': None,
            'default': default,
            'action': None,
            'msg': msg,
            'required': False,
            'has_input': not is_other
        }
        if not is_other:
            self.add_bool_kw_arg("--with-" + name.strip('-'),
                                 True, msg, True, dict_name)
            self.add_bool_kw_arg("--no-" + name.strip('-'),
                                 False, msg, True, dict_name)
        self.pos_required = False
        menu['kw_opts'][name] = arg

    def _parse(self):
        self.menus.sort(key=lambda x: len(x['name']), reverse=True)
        self._parse_menu()

    def _parse_menu(self):
        self.menu = None
        for menu in self.menus:
            menu_name = menu['name']
            if len(menu_name) > len(self.args):
                continue
            if menu_name == self.args[0:len(menu_name)]:
                self.menu = menu
                break
        if self.menu is None:
            self._invalid_menu()
        self.menu_name = self.menu['name_str']
        self.add_arg('-h',
                     default=None,
                     msg='print help message',
                     action=self._print_help,
                     aliases=['--help'])
        menu_name = self.menu['name']
        self.use_remainder = self.menu['use_remainder']
        self.args = self.args[len(menu_name):]
        self._parse_args()

    def _parse_args(self):
        self._set_defaults()
        i = self._parse_pos_args()
        self._parse_kw_args(i)

    def _set_defaults(self):
        all_opts = self.menu['pos_opts'] + list(self.menu['kw_opts'].values())
        for opt_info in all_opts:
            if opt_info['default'] is None:
                continue
            self.__dict__[opt_info['dict_name']] = opt_info['default']

    def _parse_pos_args(self):
        i = 0
        args = self.args
        menu = self.menu
        while i < len(menu['pos_opts']):
            # Get the positional arg info
            opt_name = menu['pos_opts'][i]['name']
            opt_dict_name = menu['pos_opts'][i]['dict_name']
            opt_type = menu['pos_opts'][i]['type']
            opt_choices = menu['pos_opts'][i]['choices']
            if i >= len(args):
                if i >= menu['num_required']:
                    break
                else:
                    self._missing_positional(opt_name)

            # Get the arg value
            arg = args[i]
            if arg in menu['kw_opts']:
                break
            arg = self._convert_opt(opt_name, opt_type, opt_choices, arg)

            # Set the argument
            setattr(self, opt_dict_name, arg)
            i += 1
        return i

    def _parse_kw_args(self, i):
        menu = self.menu
        args = self.args
        while i < len(args):
            # Get argument name
            opt_name = args[i]
            if opt_name not in menu['kw_opts']:
                if self.use_remainder:
                    self.remainder = " ".join(args[i:])
                    return
                else:
                    self._invalid_kwarg(opt_name)

            # Get argument type
            opt = menu['kw_opts'][opt_name]
            opt_has_input = opt['has_input']
            opt_dict_name = opt['dict_name']
            opt_type = opt['type']
            opt_default = opt['default']
            opt_action = opt['action']
            opt_choices = opt['choices']
            if not opt_has_input:
                arg = opt_default
                i += 1
            elif opt_action is not None:
                opt_action()
                arg = None
                i += 1
            elif self._next_is_kw_value(i):
                arg = args[i + 1]
                i += 2
            elif opt_default is not None:
                arg = opt_default
                i += 1
            else:
                arg = None
                self._invalid_kwarg_default(opt_name)

            # Convert argument to type
            arg = self._convert_opt(opt_name, opt_type, opt_choices, arg)

            # Set the argument
            setattr(self, opt_dict_name, arg)

    def _convert_opt(self, opt_name, opt_type, opt_choices, arg):
        if opt_type is not None:
            try:
                arg = opt_type(arg)
                if opt_choices is not None:
                    if arg not in opt_choices:
                        self._invalid_choice(opt_name, arg)
            except:
                self._invalid_type(opt_name, opt_type)
        return arg

    def _next_is_kw_value(self, i):
        if i + 1 >= len(self.args):
            return False
        return self.args[i + 1] not in self.menu['kw_opts']

    def _get_opt_name(self, opt_name, is_bool_arg=False):
        if not is_bool_arg:
            return opt_name.strip('-').replace('-', '_')
        else:
            return opt_name.replace('--with-', '', 1)\
                .replace('--no-', '', 1).\
                strip('-').replace('-', '_')

    def _invalid_menu(self):
        self._print_error(f"Could not find a menu")

    def _invalid_choice(self, opt_name, arg):
        self._print_menu_error(f"{opt_name}={arg} is not a valid choice")

    def _missing_positional(self, opt_name):
        self._print_menu_error(f"{opt_name} was required, but not defined")

    def _invalid_kwarg(self, opt_name):
        self._print_menu_error(f"{opt_name} is not a valid key-word argument")

    def _invalid_kwarg_default(self, opt_name):
        self._print_menu_error(f"{opt_name} was not given a value, but requires one")

    def _invalid_type(self, opt_name, opt_type):
        self._print_menu_error(f"{opt_name} was not of type {opt_type}")

    def _print_menu_error(self, msg):
        self._print_error(f"{self.menu['name_str']} {msg}")

    def _print_error(self, msg):
        print(f"{msg}")
        self._print_help()
        if self.exit_on_fail:
            exit(1)
        else:
            raise Exception(msg)

    def _print_help(self):
        if self.menu is not None:
            self._print_menu_help()
        else:
            self._print_menus()

    def _print_menus(self):
        for menu in self.menus:
            self.menu = menu
            self._print_menu_help(True)

    def _print_menu_help(self, only_usage=False):
        if self.menu['msg'] is not None:
            print(self.menu['msg'])
        print()
        pos_args = []
        for arg in self.menu['pos_opts']:
            if arg['required']:
                pos_args.append(f"[{arg['name']}]")
            else:
                pos_args.append(f"[{arg['name']} (opt)]")
        pos_args = " ".join(pos_args)
        menu_str = self.menu['name_str']
        print(f"USAGE: {self.binary_name} {menu_str} {pos_args} ...")
        if only_usage:
            return

        headers = ['Name', 'Default', 'Type', 'Description']
        table = []
        all_opts = self.menu['pos_opts'] + list(self.menu['kw_opts'].values())
        for arg in all_opts:
            default = arg['default']
            if self._is_bool_arg(arg):
                default = None
            table.append(
                [arg['name'], default, arg['type'], arg['msg']])
        print(tabulate(table, headers=headers))

    def _is_bool_arg(self, arg):
        return arg['type'] == bool and (arg['name'].startswith('--with-') or
                                        arg['name'].startswith('--no-'))
