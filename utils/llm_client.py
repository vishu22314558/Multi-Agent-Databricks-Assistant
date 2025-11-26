"""
LLM client wrapper for Ollama.
"""

import logging
from typing import Any, Dict, Generator, List, Optional

import ollama

logger = logging.getLogger(__name__)


class OllamaClient:
    """
    Wrapper for Ollama client with streaming and error handling.
    """
    
    def __init__(
        self,
        model: str = "llama3.1",
        base_url: str = "http://localhost:11434",
        timeout: int = 120,
    ):
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of Ollama client."""
        if self._client is None:
            self._client = ollama.Client(host=self.base_url)
        return self._client
    
    def check_connection(self) -> Dict[str, Any]:
        """
        Check connection to Ollama server.
        
        Returns:
            Dictionary with connection status
        """
        try:
            response = self.client.list()
            
            # Handle different response formats (dict or object)
            if hasattr(response, 'models'):
                # Object-style response
                models_list = response.models
                available_models = [m.model if hasattr(m, 'model') else str(m) for m in models_list]
            elif isinstance(response, dict):
                # Dict-style response
                models_list = response.get("models", [])
                available_models = [m.get("name", m.get("model", str(m))) for m in models_list]
            else:
                available_models = []
            
            return {
                "connected": True,
                "available_models": available_models,
                "selected_model": self.model,
                "model_available": any(self.model in m for m in available_models),
            }
        except Exception as e:
            logger.error(f"Failed to connect to Ollama: {e}")
            return {
                "connected": False,
                "error": str(e),
            }
    
    def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 4096,
        **kwargs
    ) -> str:
        """
        Generate a response from the model.
        
        Args:
            prompt: User prompt
            system: Optional system prompt
            temperature: Model temperature
            max_tokens: Maximum tokens to generate
            **kwargs: Additional Ollama parameters
            
        Returns:
            Generated response string
        """
        messages = []
        
        if system:
            messages.append({"role": "system", "content": system})
        
        messages.append({"role": "user", "content": prompt})
        
        try:
            response = self.client.chat(
                model=self.model,
                messages=messages,
                options={
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    **kwargs
                }
            )
            
            return response["message"]["content"]
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            raise
    
    def generate_stream(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: float = 0.1,
        **kwargs
    ) -> Generator[str, None, None]:
        """
        Generate a streaming response from the model.
        
        Args:
            prompt: User prompt
            system: Optional system prompt
            temperature: Model temperature
            **kwargs: Additional Ollama parameters
            
        Yields:
            Response tokens as they're generated
        """
        messages = []
        
        if system:
            messages.append({"role": "system", "content": system})
        
        messages.append({"role": "user", "content": prompt})
        
        try:
            stream = self.client.chat(
                model=self.model,
                messages=messages,
                stream=True,
                options={
                    "temperature": temperature,
                    **kwargs
                }
            )
            
            for chunk in stream:
                if "message" in chunk and "content" in chunk["message"]:
                    yield chunk["message"]["content"]
                    
        except Exception as e:
            logger.error(f"Error in streaming generation: {e}")
            raise
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.1,
        **kwargs
    ) -> str:
        """
        Chat with conversation history.
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            temperature: Model temperature
            **kwargs: Additional parameters
            
        Returns:
            Generated response string
        """
        try:
            response = self.client.chat(
                model=self.model,
                messages=messages,
                options={
                    "temperature": temperature,
                    **kwargs
                }
            )
            
            return response["message"]["content"]
        except Exception as e:
            logger.error(f"Error in chat: {e}")
            raise
    
    def pull_model(self, model_name: Optional[str] = None) -> bool:
        """
        Pull a model from Ollama.
        
        Args:
            model_name: Name of model to pull (default: self.model)
            
        Returns:
            True if successful
        """
        model = model_name or self.model
        
        try:
            logger.info(f"Pulling model {model}...")
            self.client.pull(model)
            logger.info(f"Model {model} pulled successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to pull model {model}: {e}")
            return False
    
    def list_models(self) -> List[str]:
        """
        List available models.
        
        Returns:
            List of model names
        """
        try:
            response = self.client.list()
            
            # Handle different response formats
            if hasattr(response, 'models'):
                models_list = response.models
                return [m.model if hasattr(m, 'model') else str(m) for m in models_list]
            elif isinstance(response, dict):
                models_list = response.get("models", [])
                return [m.get("name", m.get("model", str(m))) for m in models_list]
            return []
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            return []
    
    def get_model_info(self, model_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about a model.
        
        Args:
            model_name: Name of model (default: self.model)
            
        Returns:
            Dictionary with model information
        """
        model = model_name or self.model
        
        try:
            info = self.client.show(model)
            return {
                "name": model,
                "parameters": info.get("parameters", {}),
                "template": info.get("template", ""),
                "system": info.get("system", ""),
            }
        except Exception as e:
            logger.error(f"Failed to get model info: {e}")
            return {"name": model, "error": str(e)}


def get_ollama_client(
    model: Optional[str] = None,
    base_url: Optional[str] = None,
) -> OllamaClient:
    """
    Get an Ollama client instance.
    
    Args:
        model: Model name override
        base_url: Base URL override
        
    Returns:
        OllamaClient instance
    """
    from config.settings import get_settings
    
    settings = get_settings()
    
    return OllamaClient(
        model=model or settings.ollama.model,
        base_url=base_url or settings.ollama.base_url,
        timeout=settings.ollama.timeout,
    )
