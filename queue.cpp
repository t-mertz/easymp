#include <Python.h>

template<class T *>
class QueueItem {
public:
	T value;
	QueueItem * next;
	QueueItem * prev;

	QueueItem(T val, QueueItem * prev) :
		value(val), prev(prev), next(nullptr) {};
	QueueItem(T val) : value(val) {
		
		next = nullptr;
		prev = nullptr;
	};
};

template<class T *>
class Queue {

private:
	QueueItem * first;
	QueueItem * last;
	
	Queue(T) {};
	void put(Pyobject *) {};
	T get(T) {};
	bool empty();
};

Queue::Queue(T val) {

	first = new QueueItem(val);
	last = first;
}

Queue::put(T val) {

	QueueItem * new_item = new QueueItem(val, last);
	last = new_item;
}

T Queue::get() {

	if (first != nullptr) {
		QueueItem * temp = first;
		(*(*first).next).prev = nullptr;
		first = (*first).next;
		T val = (*temp).val;
		delete temp;
		return val;
	}
	else {
		// throw QueueEmptyError
	}
}

bool Queue::empty() {

	return (first == nullptr) ? true : false;
}