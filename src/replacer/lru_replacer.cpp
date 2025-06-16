#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::victim(frame_id_t* frame_id) {
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::scoped_lock lock{latch_}; //  如果编译报错可以替换成其他lock

    // Todo:
    //  利用lru_replacer中的LRUlist_,LRUHash_实现LRU策略
    //  选择合适的frame指定为淘汰页面,赋值给*frame_id

    // 检查LRUlist_是否为空
    if (LRUlist_.empty()) {
        return false; // 没有页面可以被淘汰
    }

    // 获取LRUlist_中最久未使用的页面
    *frame_id = LRUlist_.back();

    // 从LRUlist_和LRUHash_中移除该页面
    LRUlist_.pop_back();
    LRUhash_.erase(*frame_id);
    // LRUhash_的键是frame_id，值是LRUlist_中对应的迭代器

    return true; // 成功淘汰页面
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    // Todo:
    // 固定指定id的frame
    // 在数据结构中移除该frame

    // 如果frame_id在LRUHash_中，说明它在LRUlist_中，需要移除
    if (LRUhash_.count(frame_id)) {
        LRUlist_.erase(LRUhash_[frame_id]);
        LRUhash_.erase(frame_id);
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    // Todo:
    //  支持并发锁
    //  选择一个frame取消固定
    std::scoped_lock lock{latch_};

    // 如果frame_id已经在LRUHash_中，说明它已经可以被淘汰，无需重复插入
    if (LRUhash_.count(frame_id)) {
        return;
    }

    // 如果LRUlist_已满，不能再插入新的frame
    if (LRUlist_.size() >= max_size_) {
        return;
    }

    // 将frame_id插入到LRUlist_的头部，并更新LRUHash_
    LRUlist_.push_front(frame_id);
    LRUhash_[frame_id] = LRUlist_.begin();
}



/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }

