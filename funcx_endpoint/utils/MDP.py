"""Majordomo Protocol definitions"""
#  This is the version of MDP/Client we implement
C_CLIENT = b"MDPC01"

#  This is the version of MDP/Worker we implement
W_WORKER = b"MDPW01"

#  MDP/Server commands, as strings
W_READY         =   b"\001"
W_REQUEST       =   b"\002"
W_REPLY         =   b"\003"
W_HEARTBEAT     =   b"\004"
W_DISCONNECT    =   b"\005"

commands = [None, b"READY", b"REQUEST", b"REPLY", b"HEARTBEAT", b"DISCONNECT"]
