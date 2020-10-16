package nocomment.master.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public final class PriorityDispatchableBinaryHeap {

    /**
     * The initial capacity of the heap (2^10)
     */
    private static final int INITIAL_CAPACITY = 1024;

    /**
     * The array backing the heap
     */
    private PriorityDispatchable[] array;

    /**
     * The size of the heap
     */
    private int size;

    public PriorityDispatchableBinaryHeap() {
        this(INITIAL_CAPACITY);
    }

    public PriorityDispatchableBinaryHeap(int size) {
        this.size = 0;
        this.array = new PriorityDispatchable[size];
    }

    public final int size() {
        return size;
    }

    public final void insert(PriorityDispatchable value) {
        if (size >= array.length - 1) {
            array = Arrays.copyOf(array, array.length << 1);
        }
        size++;
        value.heapPosition = size;
        array[size] = value;
        update(value, false);
    }

    private void update(PriorityDispatchable val, boolean forceRoot) {
        int index = val.heapPosition;
        int parentInd = index >>> 1;
        PriorityDispatchable parentNode = array[parentInd];
        while (index > 1 && (forceRoot || parentNode.compareTo(val) > 0)) {
            array[index] = parentNode;
            array[parentInd] = val;
            val.heapPosition = parentInd;
            parentNode.heapPosition = index;
            index = parentInd;
            parentInd = index >>> 1;
            parentNode = array[parentInd];
        }
    }

    public final boolean remove(PriorityDispatchable val) {
        if (val.heapPosition == -1) {
            return false;
        }
        update(val, true);
        removeLowest();
        return true;
    }

    public final boolean isEmpty() {
        return size == 0;
    }

    public final PriorityDispatchable peekLowest() {
        if (size == 0) {
            throw new IllegalStateException();
        }
        return array[1];
    }

    public final PriorityDispatchable removeLowest() {
        if (size == 0) {
            throw new IllegalStateException();
        }
        PriorityDispatchable result = array[1];
        PriorityDispatchable val = array[size];
        array[1] = val;
        val.heapPosition = 1;
        array[size] = null;
        size--;
        result.heapPosition = -1;
        if (size < 2) {
            return result;
        }
        int index = 1;
        int smallerChild = 2;
        do {
            PriorityDispatchable smallerChildNode = array[smallerChild];
            if (smallerChild < size) {
                PriorityDispatchable rightChildNode = array[smallerChild + 1];
                if (smallerChildNode.compareTo(rightChildNode) > 0) {
                    smallerChild++;
                    smallerChildNode = rightChildNode;
                }
            }
            if (val.compareTo(smallerChildNode) <= 0) {
                break;
            }
            array[index] = smallerChildNode;
            array[smallerChild] = val;
            val.heapPosition = smallerChild;
            smallerChildNode.heapPosition = index;
            index = smallerChild;
        } while ((smallerChild <<= 1) <= size);
        return result;
    }

    public final Collection<PriorityDispatchable> copy() {
        return new ArrayList<>(Arrays.asList(array).subList(1, size + 1));
    }
}

