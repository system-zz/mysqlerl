%% -*- Erlang -*-

{application, mysqlerl,
 [{description, "mysqlerl"},
  {vsn, "0"},
  {modules, [mysqlerl, mysqlerl_app, mysqlerl_connection_sup,
             mysqlerl_connection]},
  {registered, [mysqlerl, mysqlerl_app, mysqlerl_connection_sup]},
  {applications, [kernel, stdlib]},
  {env, []},
  {mod, {mysqlerl_app, []}}]}.
