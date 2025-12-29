# handlers.py
import asyncio

def setup_all_handlers(app):
    """Saare handlers ek saath setup karo"""
    
    print("📝 Setting up all handlers...")
    
    # 1. Start handlers (basic commands)
    from start import setup_start_handlers
    setup_start_handlers(app)
    print("✅ Start handlers loaded")
    
    # 2. Sequence handlers (main functionality)
    from sequence import setup_sequence_handlers
    setup_sequence_handlers(app)
    print("✅ Sequence handlers loaded")
    
    # 3. Merging handlers (optional feature)
    try:
        from handler_merging import setup_merging_handlers
        setup_merging_handlers(app)
        print("✅ Merging handlers loaded")
    except Exception as e:
        print(f"⚠️ Merging handlers not loaded: {e}")
    
    print("🎉 All handlers setup complete!")
