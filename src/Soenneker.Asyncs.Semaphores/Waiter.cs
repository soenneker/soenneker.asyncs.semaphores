using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Soenneker.Queues.Intrusive.Abstractions;

namespace Soenneker.Asyncs.Semaphores;

internal sealed class Waiter : IValueTaskSource<SemaphoreLease>, IIntrusiveNode<Waiter>
{
    private const int _completedBit = 1 << 16;
    private const int _consumedBit = 1;
    private const int _dequeuedBit = 2;
    [ThreadStatic]
    private static Waiter? _localPool;

    private static readonly Action<object?> _cancelCallback = static state => ((Waiter)state!).Cancel();

    private int _state;
    private int _reclamationState;
    private ManualResetValueTaskSourceCore<SemaphoreLease> _core = new() {RunContinuationsAsynchronously = true};
    private CancellationToken _cancellationToken;
    private CancellationTokenRegistration _registration;
    private bool _cancellable;
    private short _queuedVersion;
    private Waiter? _next;

    private Waiter()
    {
    }

    public ref Waiter? Next
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref _next;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static Waiter Rent()
    {
        Waiter? waiter = _localPool;

        if (waiter is not null)
        {
            _localPool = waiter._next;
            waiter._next = null;
            return waiter;
        }

        return new Waiter();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<SemaphoreLease> NewValueTask()
        => new(this, _core.Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<SemaphoreLease> NewValueTask(CancellationToken cancellationToken)
    {
        if (cancellationToken.CanBeCanceled)
            RegisterCancellation(cancellationToken);
        else
            _cancellable = false;

        return new ValueTask<SemaphoreLease>(this, _core.Version);
    }

    private void RegisterCancellation(CancellationToken cancellationToken)
    {
        _cancellable = true;
        _queuedVersion = _core.Version;
        Volatile.Write(ref _state, (ushort)_queuedVersion);

        if (cancellationToken.IsCancellationRequested)
        {
            if (TryComplete(_core.Version))
                _core.SetException(new OperationCanceledException(cancellationToken));

            return;
        }

        _cancellationToken = cancellationToken;
        _registration = cancellationToken.UnsafeRegister(_cancelCallback, this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryComplete(short version)
        => Interlocked.CompareExchange(ref _state, (ushort)version | _completedBit, (ushort)version) == (ushort)version;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Cancel()
    {
        if (TryComplete(_queuedVersion))
            _core.SetException(new OperationCanceledException(_cancellationToken));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGrant(AsyncSemaphore semaphore)
    {
        if (_cancellable && !TryComplete(_queuedVersion))
            return false;

        _core.SetResult(new SemaphoreLease(semaphore));
        return true;
    }

    public SemaphoreLease GetResult(short token)
    {
        if (_cancellable)
            return GetCancellableResult(token);

        SemaphoreLease result = _core.GetResult(token);
        _core.Reset();
        Recycle(this);
        return result;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private SemaphoreLease GetCancellableResult(short token)
    {
        SemaphoreLease result;

        try
        {
            result = _core.GetResult(token);
        }
        catch
        {
            ResetCancellation();
            _core.Reset();
            MarkConsumed();
            throw;
        }

        ResetCancellation();
        _cancellable = false;

        _core.Reset();
        Recycle(this);
        return result;
    }

    private void ResetCancellation()
    {
        _registration.Dispose();
        _registration = default;
        _cancellationToken = default;
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ReturnUnused() => Recycle(this);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void MarkConsumed()
    {
        if ((Interlocked.Or(ref _reclamationState, _consumedBit) & _dequeuedBit) != 0)
        {
            _cancellable = false;
            Recycle(this);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void MarkDequeued()
    {
        if ((Interlocked.Or(ref _reclamationState, _dequeuedBit) & _consumedBit) != 0)
        {
            _cancellable = false;
            Recycle(this);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Recycle(Waiter waiter)
    {
        waiter._reclamationState = 0;
        waiter._next = _localPool;
        _localPool = waiter;
    }
}
