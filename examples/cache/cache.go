package cache

import (
	"sync"

	"github.com/google/uuid"
)

type Doc struct {
	ID          uuid.UUID `json:"id,omitempty"`
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
}

type Cache struct {
	c sync.Map
}

func (c *Cache) Put(k uuid.UUID, v Doc) {
	c.c.Store(k, &v)
}

func (c *Cache) Get(k uuid.UUID) *Doc {
	raw, ok := c.c.Load(k)
	if ok {
		return raw.(*Doc)
	}

	return (*Doc)(nil)
}

func (c *Cache) List() []*Doc {
	docs := make([]*Doc, 0)
	c.c.Range(func(_, value interface{}) bool {
		docs = append(docs, value.(*Doc))
		return true
	})

	return docs
}

func (c *Cache) Delete(k uuid.UUID) {
	c.c.Delete(k)
}
