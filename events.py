from abc import ABC, abstractmethod

class EventLoggable(ABC):
    @abstractmethod
    def log_notice(self, title, message):
        pass
    @abstractmethod
    def log_error(self, title, message):
        pass
    @abstractmethod
    def log_success(self, title, message):
        pass