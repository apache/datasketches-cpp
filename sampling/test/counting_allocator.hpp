/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef COUNTING_ALLOCATOR_H
#define COUNTING_ALLOCATOR_H

#include <new>
#include <utility>

namespace datasketches {

// this relies on global variables to track allocated memory
extern long long int total_allocated_memory;
extern long long int total_objects_constructed;

template <class T> class counting_allocator {
public:
  typedef T                 value_type;
  typedef value_type*       pointer;
  typedef const value_type* const_pointer;
  typedef value_type&       reference;
  typedef const value_type& const_reference;
  typedef std::size_t       size_type;
  typedef std::ptrdiff_t    difference_type;

  template <class U>
  struct rebind { typedef counting_allocator<U> other; };

  counting_allocator() {}
  counting_allocator(const counting_allocator&) {}
  template <class U>
  counting_allocator(const counting_allocator<U>&) {}
  ~counting_allocator() {}

  pointer address(reference x) const { return &x; }
  const_pointer address(const_reference x) const {
    return &x;
  }

  pointer allocate(size_type n, const_pointer = 0) {
    void* p = malloc(n * sizeof(T));
    if (!p) throw std::bad_alloc();
    total_allocated_memory += n * sizeof(T);
    return static_cast<pointer>(p);
  }

  void deallocate(pointer p, size_type n) {
    if (p) free(p);
    total_allocated_memory -= n * sizeof(T);
  }

  size_type max_size() const {
    return static_cast<size_type>(-1) / sizeof(T);
  }

  template<typename... Args>
  void construct(pointer p, Args&&... args) {
    new(p) value_type(std::forward<Args>(args)...);
    ++total_objects_constructed;
  }
  void destroy(pointer p) {
    p->~value_type();
    --total_objects_constructed;
  }

private:
  void operator=(const counting_allocator&);
};

template<> class counting_allocator<void> {
public:
  typedef void        value_type;
  typedef void*       pointer;
  typedef const void* const_pointer;

  template <class U>
  struct rebind { typedef counting_allocator<U> other; };
};


template <class T>
inline bool operator==(const counting_allocator<T>&, const counting_allocator<T>&) {
  return true;
}

template <class T>
inline bool operator!=(const counting_allocator<T>&, const counting_allocator<T>&) {
  return false;
}

}

#endif
