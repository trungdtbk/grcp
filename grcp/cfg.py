from oslo_config import cfg
from oslo_config import types

opts = [
        cfg.StrOpt('bind_host', default='0.0.0.0', help='IP address to listen on.'),
        cfg.Opt('bind_port', type=types.Integer(1024, 65535), default=9090,
            help='Port number to listen on.'),
        cfg.ListOpt('app_list', default=[], help='app module names to run.'),
        cfg.MultiStrOpt('app', default=[], positional=True, help='app module names to run.'),
        ]

CONF = cfg.ConfigOpts()
CONF.register_cli_opts(opts)



