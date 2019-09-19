import handler

import pytest

from events import stage_files_event_generator


@pytest.mark.parametrize(argnames='event, exception_expected, result_expected',
                         argvalues=[t for t in stage_files_event_generator()],
                         ids=[t for t in stage_files_event_generator(only_names=True)])
# @pytest.mark.skip()
def test__parametrize(event, exception_expected, result_expected):
    with exception_expected:
        assert handler.lambda_handler(event, '')['exception_count'] == result_expected


# @pytest.mark.parametrize('event_factory, except_output, exception_handler', [
#     (list_events[3], 0, does_not_raise()),
# ],
#                          indirect=['event_factory'],
#                          ids=[list_events[3]]
#                          )
# def test__lambda_handler__fruit_count_summary(event_factory, except_output, exception_handler):
#     with exception_handler:
#         if callable(event_factory):
#             event = event_factory()
#         assert lambda_handler(event, '')["exception_count"] == except_output