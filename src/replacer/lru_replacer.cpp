#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::victim(frame_id_t* frame_id) {
    std::scoped_lock lock{latch_};

    // 检查LRU列表是否为空
    if (LRUlist_.empty()) {
        return false;  // 无页面可淘汰
    }

    // 获取LRU列表中最久未使用的页面（列表尾部）
    *frame_id = LRUlist_.back();

    // 从哈希表中移除该页面
    LRUhash_.erase(*frame_id);

    // 从列表中移除该页面
    LRUlist_.pop_back();

    return true;  // 成功淘汰页面
}

void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};

    auto it = LRUhash_.find(frame_id);
    if (it != LRUhash_.end()) {
        LRUlist_.erase(it->second);  // 直接使用存储的迭代器删除
        LRUhash_.erase(it);
    }
}

void LRUReplacer::unpin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};

    if (LRUhash_.find(frame_id) == LRUhash_.end()) {
        LRUlist_.push_front(frame_id);
        LRUhash_[frame_id] = LRUlist_.begin();  // 存储迭代器
    }
}
size_t LRUReplacer::Size() {
    std::scoped_lock lock{latch_};
    return LRUlist_.size();
}