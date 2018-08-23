-include("brod.hrl").
-include_lib("proper/include/proper.hrl").

%% `CLIENT_ID' is used by the test suite to control
-define(TEST_CLIENT_ID, test_client).
-define(TEST_TOPIC, <<"brod_consumer_prop_topic">>).

-define(BOOTSTRAP_HOSTS, [{"localhost", 9092}]).
