using Soenneker.Asyncs.Semaphores.Abstract;
using Soenneker.Queues.Intrusive.ValueMpsc;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Semaphores;

public sealed class AsyncSemaphore : IAsyncSemaphore
{
    private const long _countMask = uint.MaxValue;
    private const long _handoffUnit = 1L << 32;

    // Low 32 bits: signed permit/waiter count. High 32 bits: handoffs awaiting the single queue consumer.
    private long _state;
    private int _useOverflowQueue;
    private Waiter? _frontWaiter;
    private ValueIntrusiveMpscReclaimingQueue<Waiter> _waiterQueue;

    /// <summary>
    /// Creates a semaphore whose initial and maximum permit counts are both <paramref name="count"/>.
    /// </summary>
    /// <param name="count">The number of permits available initially and the maximum number of permits.</param>
    public AsyncSemaphore(int count) : this(count, count)
    {
    }

    /// <summary>
    /// Creates a semaphore with the specified initial and maximum permit counts.
    /// </summary>
    /// <param name="initialCount">The number of permits initially available.</param>
    /// <param name="maxCount">The maximum number of permits.</param>
    public AsyncSemaphore(int initialCount, int maxCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(initialCount);

        if (initialCount > maxCount)
            throw new ArgumentOutOfRangeException(nameof(initialCount), initialCount, "The initial count cannot exceed the maximum count.");

        _state = (uint)initialCount;
        MaxCount = maxCount;

        Waiter stub = Waiter.Rent();
        stub.Next = null;
        _waiterQueue = new ValueIntrusiveMpscReclaimingQueue<Waiter>(stub);
    }

    public int CurrentCount
    {
        get
        {
            int count = GetCount(Volatile.Read(ref _state));
            return count > 0 ? count : 0;
        }
    }

    public int MaxCount { get; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<SemaphoreLease> Acquire()
    {
        long state = Volatile.Read(ref _state);

        while (GetCount(state) > 0)
        {
            long observed = Interlocked.CompareExchange(ref _state, state - 1, state);

            if (observed == state)
                return new ValueTask<SemaphoreLease>(new SemaphoreLease(this));

            state = observed;
        }

        return AcquireSlow(state);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<SemaphoreLease> Acquire(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<SemaphoreLease>(cancellationToken);

        long state = Volatile.Read(ref _state);

        while (GetCount(state) > 0)
        {
            long observed = Interlocked.CompareExchange(ref _state, state - 1, state);

            if (observed == state)
                return new ValueTask<SemaphoreLease>(new SemaphoreLease(this));

            state = observed;
        }

        return AcquireSlow(state, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<SemaphoreLease> AcquireSlow(long state)
    {
        Waiter waiter = Waiter.Rent();

        if (DecrementCount(state) >= 0)
        {
            waiter.ReturnUnused();
            return new ValueTask<SemaphoreLease>(new SemaphoreLease(this));
        }

        ValueTask<SemaphoreLease> result = waiter.NewValueTask();
        Publish(waiter);
        return result;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<SemaphoreLease> AcquireSlow(long state, CancellationToken cancellationToken)
    {
        Waiter waiter = Waiter.Rent();

        if (DecrementCount(state) >= 0)
        {
            waiter.ReturnUnused();
            return new ValueTask<SemaphoreLease>(new SemaphoreLease(this));
        }

        ValueTask<SemaphoreLease> result = waiter.NewValueTask(cancellationToken);
        Publish(waiter);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Publish(Waiter waiter)
    {
        if (Volatile.Read(ref _useOverflowQueue) != 0 ||
            Interlocked.CompareExchange(ref _frontWaiter, waiter, null) is not null)
        {
            Volatile.Write(ref _useOverflowQueue, 1);
            _waiterQueue.Enqueue(waiter);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAcquire(out SemaphoreLease lease)
    {
        if (TryTakePermit())
        {
            lease = new SemaphoreLease(this);
            return true;
        }

        lease = default;
        return false;
    }

    public int Release() => ReleaseCore(1);

    public int Release(int releaseCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(releaseCount, 1);
        return ReleaseCore(releaseCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ReleaseLease()
    {
        long state = Volatile.Read(ref _state);

        if ((ulong)state <= int.MaxValue)
        {
            long observed = Interlocked.CompareExchange(ref _state, state + 1, state);

            if (observed == state)
                return;

            state = observed;
        }

        if (state == uint.MaxValue)
        {
            long observed = Interlocked.CompareExchange(ref _state, 0, state);

            if (observed == state)
            {
                ReleaseFinalWaiter();
                return;
            }

            state = observed;
        }

        ReleaseLeaseSlow(state);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ReleaseLeaseSlow(long state)
    {
        while (true)
        {
            int count = GetCount(state);
            int pending = GetPendingHandoffs(state);
            bool direct = count == -1 && pending == 0;
            long updated = Compose(count + 1, direct || count >= 0 ? pending : pending + 1);
            long observed = Interlocked.CompareExchange(ref _state, updated, state);

            if (observed != state)
            {
                state = observed;
                continue;
            }

            if (count >= 0)
                return;

            if (direct)
                ReleaseFinalWaiter();
            else if (pending == 0)
                DrainWaiters(1);

            return;
        }
    }

    private int ReleaseCore(int releaseCount)
    {
        long state = Volatile.Read(ref _state);

        while (true)
        {
            int count = GetCount(state);
            int pending = GetPendingHandoffs(state);
            int available = count > 0 ? count : 0;

            if (releaseCount > MaxCount - available)
                throw new SemaphoreFullException();

            int handoffs = Math.Min(releaseCount, -Math.Min(count, 0));
            bool direct = handoffs == 1 && count + releaseCount == 0 && pending == 0;
            long updated = Compose(count + releaseCount, pending + (direct ? 0 : handoffs));
            long observed = Interlocked.CompareExchange(ref _state, updated, state);

            if (observed != state)
            {
                state = observed;
                continue;
            }

            if (direct)
                ReleaseFinalWaiter();
            else if (handoffs != 0 && pending == 0)
                DrainWaiters(handoffs);

            return available;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryTakePermit()
    {
        long state = Volatile.Read(ref _state);

        while (GetCount(state) > 0)
        {
            long updated = state - 1;
            long observed = Interlocked.CompareExchange(ref _state, updated, state);

            if (observed == state)
                return true;

            state = observed;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int DecrementCount(long state)
    {
        if (state == 0)
        {
            long observed = Interlocked.CompareExchange(ref _state, uint.MaxValue, 0);

            if (observed == 0)
                return -1;

            state = observed;
        }

        while (true)
        {
            int count = GetCount(state) - 1;
            long observed = Interlocked.CompareExchange(ref _state, WithCount(state, count), state);

            if (observed == state)
                return count;

            state = observed;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReleaseFinalWaiter()
    {
        if (Volatile.Read(ref _useOverflowQueue) != 0)
        {
            AddHandoff();
            return;
        }

        Waiter? waiter = TakeFrontWaiter();

        if (waiter is null)
        {
            AddHandoff();
            return;
        }

        if (waiter.TryGrant(this))
            return;

        waiter.MarkDequeued();
        RestoreCanceledHandoff();
    }

    private void AddHandoff()
    {
        long state = Interlocked.Add(ref _state, _handoffUnit);

        if (GetPendingHandoffs(state) == 1)
            DrainWaiters(1);
    }

    private void RestoreCanceledHandoff()
    {
        long state = Volatile.Read(ref _state);

        while (true)
        {
            int count = GetCount(state) + 1;
            long observed = Interlocked.CompareExchange(ref _state, WithCount(state, count), state);

            if (observed != state)
            {
                state = observed;
                continue;
            }

            if (count == 0)
                ReleaseFinalWaiter();
            else if (count < 0)
                AddHandoff();

            return;
        }
    }

    private void DrainWaiters(int claimedHandoffs)
    {
        var spinner = new SpinWait();

        while (true)
        {
            int remaining = claimedHandoffs;

            while (remaining-- > 0)
            {
                Waiter? waiter = TakeFrontWaiter();

                if (waiter is null)
                {
                    while (!_waiterQueue.TryDequeueSpinUntilLinked(out waiter) &&
                           (waiter = TakeFrontWaiter()) is null)
                        spinner.SpinOnce();
                }

                if (waiter.TryGrant(this))
                    continue;

                waiter.MarkDequeued();
                IncrementCountOnly();
                remaining++;
            }

            long state = Volatile.Read(ref _state);

            while (true)
            {
                int pending = GetPendingHandoffs(state);
                int next = pending - claimedHandoffs;
                long observed = Interlocked.CompareExchange(ref _state, state - ((long)claimedHandoffs << 32), state);

                if (observed == state)
                {
                    if (next == 0)
                        return;

                    claimedHandoffs = next;
                    break;
                }

                state = observed;
            }
        }
    }

    private void IncrementCountOnly()
    {
        long state = Volatile.Read(ref _state);

        while (true)
        {
            long observed = Interlocked.CompareExchange(ref _state, WithCount(state, GetCount(state) + 1), state);

            if (observed == state)
                return;

            state = observed;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Waiter? TakeFrontWaiter()
    {
        Waiter? waiter = Volatile.Read(ref _frontWaiter);

        if (waiter is not null)
            Volatile.Write(ref _frontWaiter, null);

        return waiter;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetCount(long state) => unchecked((int)state);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetPendingHandoffs(long state) => (int)((ulong)state >> 32);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long WithCount(long state, int count) => (state & ~_countMask) | (uint)count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Compose(int count, int pendingHandoffs) => ((long)pendingHandoffs << 32) | (uint)count;
}
