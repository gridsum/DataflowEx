using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// Represents a unit/child of a container whose lifecycle should be observed and managed
    /// by a host container
    /// </summary>
    public interface IChildMeta
    {
        IEnumerable<IDataflowBlock> Blocks { get; }
        Task ChildCompletion { get; }
        int BufferCount { get; }
        string DisplayName { get; }
    }

    /// <summary>
    /// A block container can have block as its 
    /// </summary>
    internal class BlockMeta : IChildMeta
    {
        private readonly IDataflowBlock m_block;
        private readonly Task m_completion;

        public BlockMeta(IDataflowBlock block, Task completion)
        {
            m_block = block;
            m_completion = completion;
        }

        public IDataflowBlock Block { get { return m_block; } }

        public IEnumerable<IDataflowBlock> Blocks { get { return new [] {m_block}; } }
        public Task ChildCompletion { get { return m_completion; } }
        public int BufferCount { get { return m_block.GetBufferCount(); } }
        public string DisplayName { get { return Utils.GetFriendlyName(m_block.GetType()); } }
    }

    internal class BlockContainerMeta : IChildMeta
    {
        private readonly BlockContainer m_container;
        private readonly Task m_completion;

        public BlockContainerMeta(BlockContainer container, Task completion)
        {
            m_container = container;
            m_completion = completion;
        }

        public IEnumerable<IDataflowBlock> Blocks { get { return m_container.Blocks; } }
        public Task ChildCompletion { get { return m_completion; } }
        public int BufferCount { get { return m_container.BufferedCount; } }
        public string DisplayName { get { return m_container.Name; } }
    }
}
