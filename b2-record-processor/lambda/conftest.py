import pytest


@pytest.fixture(scope="function")
def record_processor_instance():
    from models.record_processor import RecordProcessorBase
    import config

    class RecordProcessorTest(RecordProcessorBase):
        _tag_id: str
        _types_to_be_processed: list
        _message_payload: dict

        def types_to_be_processed(self) -> list:
            return self._types_to_be_processed

        def create_message_payload(self, payload) -> iter:
            for i in range(0, 1):
                yield self._message_payload

        def get_tag_id(self) -> str:
            return self._tag_id

    return RecordProcessorTest(name='PyTestProcessor', cloud_provider=config.SYS_CLOUD_PROVIDER)


@pytest.fixture(scope="function")
def wave_valid_event():
    from events import stage_files_event_generator
    event = next(stage_files_event_generator(file_type_filter='wave'))[0]
    return event['Records'][0]
