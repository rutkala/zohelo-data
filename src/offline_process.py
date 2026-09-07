"""Run a local command with kernel-enforced network denial.

Shared by native metric execution and credential-free verification. Linux-only;
fails closed if the filter cannot be installed. Does not modify installed
packages or fake network replies.
"""
import ctypes
import errno
import os
import sys

lib = ctypes.CDLL('libseccomp.so.2', use_errno=True)
lib.seccomp_init.argtypes = [ctypes.c_uint32]
lib.seccomp_init.restype = ctypes.c_void_p
lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
lib.seccomp_syscall_resolve_name.restype = ctypes.c_int
lib.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
lib.seccomp_rule_add.restype = ctypes.c_int
lib.seccomp_load.argtypes = [ctypes.c_void_p]
lib.seccomp_load.restype = ctypes.c_int
lib.seccomp_release.argtypes = [ctypes.c_void_p]
lib.seccomp_release.restype = None

ctx = lib.seccomp_init(0x7fff0000)
if not ctx:
    raise SystemExit('Cannot initialize offline syscall filter')
try:
    for name in (b'socket', b'socketpair', b'connect', b'sendto', b'sendmsg', b'sendmmsg'):
        number = lib.seccomp_syscall_resolve_name(name)
        if number < 0 or lib.seccomp_rule_add(ctx, 0x00050000 | errno.ENETUNREACH, number, 0) != 0:
            raise SystemExit('Cannot configure offline syscall filter')
    if lib.seccomp_load(ctx) != 0:
        raise SystemExit('Cannot activate offline syscall filter')
finally:
    lib.seccomp_release(ctx)

os.environ['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'false'
if len(sys.argv) < 2:
    raise SystemExit('Usage: offline_process.py PROGRAM [ARGUMENTS]')
os.execv(sys.argv[1], sys.argv[1:])
