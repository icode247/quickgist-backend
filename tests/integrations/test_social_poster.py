import unittest
from unittest.mock import patch, MagicMock, PropertyMock

# Assuming 'scripts.backend.integrations' and 'scripts.backend.integrations.utils' are discoverable
from scripts.backend.integrations.social_poster import SocialPoster, BasePlatformPoster
# Import encryption utils for mocking and potentially for generating test values if needed
from scripts.backend.integrations.utils.encryption_utils import get_encryption_key, encrypt, decrypt

# A dummy platform for testing BasePlatformPoster directly if needed, or for mocking
class MockPlatformPoster(BasePlatformPoster):
    def _verify_logged_in(self, driver, platform): return True
    def _fresh_login(self, driver, platform, username, password): return {'success': True}
    def _publish_post(self, driver, post_data, platform): return {'success': True, 'platform_post_id': 'mock_post_123'}


class TestBasePlatformPoster(unittest.TestCase):

    def setUp(self):
        self.mock_session_manager = MagicMock()
        # Instantiate BasePlatformPoster with the mocked session manager
        # Normally BasePlatformPoster is subclassed, but we can test its methods
        # by creating a concrete instance or a simple mock subclass.
        self.poster = MockPlatformPoster(session_manager=self.mock_session_manager)

        # Mock the _setup_driver method for BasePlatformPoster
        self.mock_driver = MagicMock(name="mock_driver")
        self.poster._setup_driver = MagicMock(return_value=self.mock_driver)


    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_connection_with_session_restore_success(self, mock_get_key):
        mock_get_key.return_value = b"test_encryption_key_bytes_32_len" # 32 bytes
        self.mock_session_manager.load_session.return_value = {'cookies': [], 'current_url': 'http://example.com'}
        self.mock_session_manager.restore_session.return_value = True
        # _verify_logged_in is part of MockPlatformPoster, assuming it returns True for this path

        result = self.poster.test_connection_with_session('user1', 'mockplatform', 'user', 'enc_pass')

        self.assertTrue(result['success'])
        self.assertEqual(result['message'], 'Session restored successfully')
        self.mock_session_manager.load_session.assert_called_once_with('user1', 'mockplatform')
        self.mock_session_manager.restore_session.assert_called_once_with(self.mock_driver, {'cookies': [], 'current_url': 'http://example.com'})
        self.poster._setup_driver.assert_called_once() # Driver should be set up
        self.mock_driver.quit.assert_called_once() # Driver should be quit

    @patch('scripts.backend.integrations.social_poster.decrypt')
    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_connection_with_session_fresh_login_success(self, mock_get_key, mock_decrypt):
        mock_get_key.return_value = b"test_key_bytes_32_len_for_test"
        mock_decrypt.return_value = "plain_password"

        self.mock_session_manager.load_session.return_value = None # No session
        # _fresh_login from MockPlatformPoster returns {'success': True}
        self.mock_session_manager.save_session.return_value = True

        result = self.poster.test_connection_with_session('user1', 'mockplatform', 'user', 'encrypted_pass_str')

        self.assertTrue(result['success'])
        self.assertEqual(result['message'], 'Login successful, session saved')
        mock_decrypt.assert_called_once_with('encrypted_pass_str', b"test_key_bytes_32_len_for_test")
        # self.poster._fresh_login mock is implicitly called via self.poster instance
        self.mock_session_manager.save_session.assert_called_once_with('user1', 'mockplatform', self.mock_driver)
        self.poster._setup_driver.assert_called_once()
        self.mock_driver.quit.assert_called_once()

    @patch('scripts.backend.integrations.social_poster.decrypt')
    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_connection_with_session_decryption_fails(self, mock_get_key, mock_decrypt):
        mock_get_key.return_value = b"test_key_bytes_for_this_test"
        mock_decrypt.side_effect = ValueError("Decryption error from mock")

        self.mock_session_manager.load_session.return_value = None

        result = self.poster.test_connection_with_session('user1', 'mockplatform', 'user', 'bad_encrypted_pass')

        self.assertFalse(result['success'])
        self.assertIn('Failed to decrypt password', result['error'])
        mock_decrypt.assert_called_once_with('bad_encrypted_pass', b"test_key_bytes_for_this_test")
        self.poster._setup_driver.assert_called_once()
        self.mock_driver.quit.assert_called_once()

    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_connection_with_session_key_error(self, mock_get_key):
        mock_get_key.side_effect = ValueError("Simulated key error")

        result = self.poster.test_connection_with_session('user1', 'mockplatform', 'user', 'any_pass')

        self.assertFalse(result['success'])
        self.assertIn('Encryption key not configured or invalid', result['error'])
        self.assertTrue(result.get('key_error'))
        # Driver should not be set up if key retrieval fails before it
        self.poster._setup_driver.assert_not_called()


class TestSocialPoster(unittest.TestCase):

    def setUp(self):
        self.mock_redis_client = MagicMock()
        self.social_poster = SocialPoster(redis_client=self.mock_redis_client)
        # Mock the platform specific poster instances within social_poster.platforms
        self.mock_platform_instance = MagicMock(spec=BasePlatformPoster)
        self.social_poster.platforms['testplatform'] = self.mock_platform_instance

    @patch('scripts.backend.integrations.social_poster.encrypt')
    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_account_connection_success(self, mock_get_key, mock_encrypt):
        mock_get_key.return_value = b"a_valid_key_for_encrypt_32b"
        mock_encrypt.return_value = "encrypted_password_mock_output"
        self.mock_platform_instance.test_connection_with_session.return_value = {'success': True, 'message': 'Platform connected'}

        result = self.social_poster.test_account_connection('testplatform', 'testuser', 'plain_pass', 'user123')

        self.assertTrue(result['success'])
        self.assertEqual(result['message'], 'Platform connected')
        mock_get_key.assert_called_once()
        mock_encrypt.assert_called_once_with('plain_pass', b"a_valid_key_for_encrypt_32b")
        self.mock_platform_instance.test_connection_with_session.assert_called_once_with(
            'user123', 'testplatform', 'testuser', "encrypted_password_mock_output"
        )

    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_account_connection_key_error(self, mock_get_key):
        mock_get_key.side_effect = ValueError("Cannot get key")

        result = self.social_poster.test_account_connection('testplatform', 'testuser', 'plain_pass', 'user123')

        self.assertFalse(result['success'])
        self.assertTrue(result.get('key_error'))
        self.assertIn('Encryption key not configured or invalid', result['error'])
        self.mock_platform_instance.test_connection_with_session.assert_not_called()

    @patch('scripts.backend.integrations.social_poster.encrypt')
    @patch('scripts.backend.integrations.social_poster.get_encryption_key')
    def test_test_account_connection_encryption_failure(self, mock_get_key, mock_encrypt):
        mock_get_key.return_value = b"another_valid_32_byte_key_!!"
        mock_encrypt.side_effect = Exception("Encryption process failed") # Generic exception

        result = self.social_poster.test_account_connection('testplatform', 'testuser', 'plain_pass', 'user123')

        self.assertFalse(result['success'])
        self.assertEqual(result['error'], 'Failed to prepare credentials for testing.')
        mock_encrypt.assert_called_once_with('plain_pass', b"another_valid_32_byte_key_!!")
        self.mock_platform_instance.test_connection_with_session.assert_not_called()

    # Tests for SocialPoster.publish_post (focusing on new error handling)
    @patch.object(SocialPoster, '_mark_account_disconnected') # Patching the method on the class
    def test_publish_post_handles_key_error_from_poster(self, mock_mark_disconnected):
        account_data = {'id': 'acc1', 'user_id': 'user1', 'platform': 'testplatform', 'username': 'testuser', 'connected': True, 'password_encrypted': '...'}
        post_data = {'platform': 'testplatform', 'content': 'hello'}

        # Simulate publish_with_session returning a key_error
        self.mock_platform_instance.publish_with_session.return_value = {
            'success': False,
            'error': 'Encryption key not configured',
            'key_error': True
        }

        self.social_poster.publish_post(post_data, account_data)

        mock_mark_disconnected.assert_called_once_with('acc1', 'Encryption key not configured')

    @patch.object(SocialPoster, '_mark_account_disconnected')
    def test_publish_post_handles_decryption_error_flag_from_poster(self, mock_mark_disconnected):
        # Let's assume 'decryption_error' is a hypothetical flag we might add later for more specificity
        # For now, the prompt implies requires_reconnection or error message content would catch it.
        # This test demonstrates how it *could* be handled if such a flag existed.
        # The current implementation relies on 'requires_reconnection' or error string parsing.
        account_data = {'id': 'acc1', 'user_id': 'user1', 'platform': 'testplatform', 'username': 'testuser', 'connected': True, 'password_encrypted': '...'}
        post_data = {'platform': 'testplatform', 'content': 'hello'}

        self.mock_platform_instance.publish_with_session.return_value = {
            'success': False,
            'error': 'Password decryption failed',
            'decryption_error': True # Hypothetical flag
        }
        self.social_poster.publish_post(post_data, account_data)
        mock_mark_disconnected.assert_called_once_with('acc1', 'Password decryption failed')


    @patch.object(SocialPoster, '_mark_account_disconnected')
    def test_publish_post_handles_requires_reconnection_flag(self, mock_mark_disconnected):
        account_data = {'id': 'acc1', 'user_id': 'user1', 'platform': 'testplatform', 'username': 'testuser', 'connected': True, 'password_encrypted': '...'}
        post_data = {'platform': 'testplatform', 'content': 'hello'}

        self.mock_platform_instance.publish_with_session.return_value = {
            'success': False,
            'error': 'Session expired, re-login failed.',
            'requires_reconnection': True
        }
        self.social_poster.publish_post(post_data, account_data)
        mock_mark_disconnected.assert_called_once_with('acc1', 'Session expired, re-login failed.')

    # TODO: (Optional) Add tests for BasePlatformPoster.publish_with_session
    # These would be similar to test_connection_with_session but would also mock _publish_post
    # and test the various paths (session restore, re-login success, re-login failure paths).

if __name__ == "__main__":
    unittest.main()
