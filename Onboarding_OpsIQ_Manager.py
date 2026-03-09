from json import dumps
from boto3 import client
import uuid
import Onboarding_Log_Manager

log = Onboarding_Log_Manager.get_module_logger('OpsIQUtility')


# The following class should be included into another different Utility file
class SQSOperations:

    def __init__(self, queue_name):
        """Init SQSOperations object"""

        self.sqs_client = client('sqs')
        self.queue_name = queue_name
        self.queue_url = None


    def set_queue_url(self, queue_name: str):
        """Set the Queue URL for the message operation to be performed"""

        response = self.sqs_client.get_queue_url(QueueName=queue_name)
        self.queue_url = response['QueueUrl']


    def push_message(self, message, queue = None):
        """Push the message to the SQS configured"""

        if queue is None:
            queue = self.queue_name

        self.set_queue_url(queue)
        response = self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=message)
        log.info(f"Message published successfully with id {response['MessageId']}:- {message} to {queue}")
# Up here.



class OpsIQMessageManager(SQSOperations):

    def __init__(self, opsiq_queue, batch_name, field_schema, transaction_id, system, source, source_bucket, config_template):
        """Init the OpsIQMessageManager Class"""

        super().__init__(opsiq_queue)  # Initiate the parent class

        self.batch_id = batch_name
        self.field_schema = field_schema
        self.transaction_id = transaction_id
        self.system = system
        self.source = source
        self.list_payloads = []
        self.source_bucket = source_bucket
        self.config_template = config_template


    # INIT: OPSIQ Configuration Step 1: Message construction
    @staticmethod
    def create_item_message_content(func):
        """Create item message content for the failed items in Onboarding or OPEX, (OPEX includes not only failed items, but also successful items in the same batch for the OpsIQ message)"""

        def wrapper(self, **kwargs):

            log.info("The OpsIQ 'create_item_message_content' function received:\n\n"
                f"- kwargs: '{kwargs}'\n")

            item_payload = {
                'Id': str(uuid.uuid1()),  # uuid7str()
                'ItemCode': kwargs.get('ItemCode', None),
                'Status': kwargs.get('Status', None),
                'Error': kwargs.get('Error', None),
                'ErrorCode': kwargs.get('ErrorCode', None),
                'EventName': kwargs.get('EventName', None),
                'FieldList': kwargs.get('FieldList', None),
                'DocumentImages': [element for element in (kwargs.get('DocumentImages', None) or [])],
            }

            log.info('The creation of the Item message for OpsIQ is ready.\n')
            return func(self, item_payload=item_payload)

        return wrapper


    @create_item_message_content
    def save_item_message_content(self, **kwargs):
        """Function to save the item message content into the class list"""

        item_payload = kwargs.get('item_payload', None)

        log.info(f"Item payload result:\n\n'{dumps(item_payload, indent=4)}'\n")

        log.info('Appending the Item message created to the list of payloads...')
        self.list_payloads.append(item_payload)
        log.info(f"Item message successfully appended.\n{'*'*50}\n\n")
    # END: OPSIQ Configuration Step 1: Message construction


    # INIT: OPSIQ Configuration Step 2: Sending the Message
    @staticmethod
    def create_main_message_headers(func):
        """Create the main message content headers for Onboarding or OPEX, (Both share the same headers as they are using the universal structure)"""

        def wrapper(self, **kwargs):

            log.info("The OpsIQ 'create_main_message_headers' function received:\n"
                     f"- kwargs: '{kwargs}'\n")

            final_payload = {
                'BatchId': self.batch_id,  # alegant_aaaa_12315
                'FieldSchema': self.field_schema,  # List of dicts
                'TransactionId': self.transaction_id,  # 485ab8b9-b1d6-4989-a619-7777777
                'System': self.system,  # opsiq
                'Source': self.source,  # Onboarding
                'Items': self.list_payloads,
                'ConfigTemplate': self.config_template
            }

            log.info('The creation of the message for OpsIQ is ready.\n')
            return func(self, final_payload=final_payload)

        return wrapper


    @create_main_message_headers
    def send_opsiq_message(self, **kwargs):

        final_payload = kwargs.get('final_payload', None)

        log.info(f"Final payload result:\n\n'{dumps(final_payload, indent=4)}'\n")

        log.info('Pushing the final OpsIQ message to the OpsIQ SQS...')
        self.push_message(dumps(final_payload))
        log.info('Final OpsIQ message was pushed.')
    # END: OPSIQ Configuration Step 2: Sending the Message
