import os
# NOTE: Need to set these values as part of the environment variables
# using export MAIL_SERVER=.... and so on
MAIL_CONFIG = {
    'mail_server': os.environ.get('MAIL_SERVER'), #'smtp.gmail.com',
    'mail_port': os.environ.get('MAIL_PORT'), #465,
    'mail_username': os.environ.get('MAIL_USERNAME'), #'uribbas@gmail.com',
    'mail_password': os.environ.get('MAIL_PASSWORD'), #'mailpassword'
};
