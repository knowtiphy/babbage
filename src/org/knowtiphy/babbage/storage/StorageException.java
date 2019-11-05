package org.knowtiphy.babbage.storage;

/**
 *
 * @author graham
 */
public class StorageException extends Exception
{
	private static final long serialVersionUID = 1L;

    public StorageException()
    {
    }

    public StorageException(String message)
    {
        super(message);
    }

    public StorageException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public StorageException(Throwable cause)
    {
        super(cause);
    }

    public StorageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
