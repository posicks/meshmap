package jamsesso.meshmap;

@FunctionalInterface
public interface Handler<T> 
{
    public T handle(T response);
}
