{:ok, _apps} = Application.ensure_all_started(:inets)

ExUnit.start()
