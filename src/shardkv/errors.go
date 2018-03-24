package shardkv

import "errors"

var ErrLegacyConf = errors.New("Legacy Conf")
var ErrDupCall = errors.New("Dup Call")
var ErrNotFound = errors.New("Key Not Found")
var ErrNotLeader = errors.New("Not Leader")
var ErrWrongServer = errors.New("Wrong Server")
