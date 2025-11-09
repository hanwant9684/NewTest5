"""
Telegram bot with minimal WSGI server (replaces Flask for ~15-20MB RAM savings)
Uses Python's built-in wsgiref - zero dependencies
Optimized for constrained environments (Render 512MB, Replit)
"""
import os
import sys
from urllib.parse import parse_qs
from html import escape
from logger import LOGGER

# Initialize module logger
_logger = LOGGER(__name__)

def load_landing_page(session_id):
    """Landing page shown before ad verification - prevents premature code generation"""
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="robots" content="noindex, nofollow">
    <title>Complete Verification</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }}
        .container {{ background: white; border-radius: 20px; padding: 40px; max-width: 500px; width: 100%; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); text-align: center; }}
        .icon {{ font-size: 64px; margin-bottom: 20px; animation: scaleIn 0.5s ease-out; }}
        @keyframes scaleIn {{ from {{ transform: scale(0); opacity: 0; }} to {{ transform: scale(1); opacity: 1; }} }}
        h1 {{ color: #2d3748; margin-bottom: 15px; font-size: 28px; }}
        .message {{ color: #718096; margin-bottom: 30px; font-size: 16px; line-height: 1.6; }}
        .instructions {{ background: #edf2f7; border-radius: 12px; padding: 20px; text-align: left; margin: 25px 0; }}
        .instructions h3 {{ color: #2d3748; font-size: 18px; margin-bottom: 15px; }}
        .instructions ol {{ color: #4a5568; padding-left: 20px; line-height: 1.8; }}
        .btn {{ border: none; padding: 16px 40px; border-radius: 8px; font-size: 18px; font-weight: 600; cursor: pointer; margin: 10px 5px; transition: all 0.3s ease; display: inline-block; text-decoration: none; min-width: 250px; }}
        .btn-primary {{ background: #48bb78; color: white; }}
        .btn-primary:hover {{ background: #38a169; transform: translateY(-2px); box-shadow: 0 6px 20px rgba(72, 187, 120, 0.4); }}
        @media (max-width: 500px) {{
            .container {{ padding: 30px 20px; }}
            h1 {{ font-size: 24px; }}
            .btn {{ min-width: 100%; margin: 8px 0; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">🎯</div>
        <h1>Almost There!</h1>
        <p class="message">You've successfully completed the ad. Click the button below to get your verification code.</p>
        
        <div class="instructions">
            <h3>📋 What happens next?</h3>
            <ol>
                <li>Click "Get Verification Code" below</li>
                <li>Copy your unique code</li>
                <li>Go back to the Telegram bot</li>
                <li>Enter the code to unlock premium downloads</li>
            </ol>
        </div>
        
        <a href="/verify-ad?session={escape(session_id)}&confirm=1" class="btn btn-primary">✅ Get Verification Code</a>
    </div>
</body>
</html>'''
    return html

def load_template(code, title, message, bot_username):
    """Minimal HTML template - replaces Jinja2"""
    icon = '✅' if code else '❌'
    
    if code:
        auto_verify_html = ''
        if bot_username:
            auto_verify_html = f'<a href="https://t.me/{escape(bot_username)}?start=verify_{escape(code)}" class="btn btn-primary" id="autoVerifyBtn" onclick="trackClick(\'auto_verify\')">✅ Auto-Verify in Bot</a><p style="margin: 15px 0; color: #718096; font-size: 14px;">Recommended: One-click verification ☝️</p>'
        else:
            auto_verify_html = '<div class="alert alert-info show">ℹ️ Bot username not configured. Use manual verification below.</div>'
        
        code_section = f'''
    <div class="code-box" id="codeBox">
        <div class="code-label">Your Verification Code</div>
        <div class="verification-code" id="code" onclick="copyCode()" title="Click to copy">{escape(code)}</div>
    </div>
    
    <div class="timer-warning" id="timerWarning">
        ⏰ This code expires in <span id="timeRemaining">30:00</span> minutes
    </div>
    
    {auto_verify_html}
    
    <button class="btn btn-secondary" id="copyBtn" onclick="copyCode()">📋 Copy Code</button>
    
    <div class="instructions">
        <h3>📱 Manual Verification:</h3>
        <ol>
            <li>Go back to the Telegram bot</li>
            <li>Send this command:<br><code id="manualCommand">/verifypremium {escape(code)}</code></li>
            <li>Enjoy your free downloads!</li>
        </ol>
    </div>
    
    <div style="margin-top: 20px; padding: 15px; background: #f7fafc; border-radius: 8px;">
        <p style="font-size: 14px; color: #4a5568; margin-bottom: 10px;">
            <strong>💡 Troubleshooting:</strong>
        </p>
        <ul style="text-align: left; font-size: 13px; color: #718096; padding-left: 25px;">
            <li>Code saved in your browser for safety</li>
            <li>Don't refresh this page - code will expire</li>
            <li>If button doesn't work, copy code manually</li>
            <li>Code valid for 30 minutes only</li>
        </ul>
    </div>
    '''
    else:
        code_section = f'''
    <div class="instructions">
        <h3>❓ What happened?</h3>
        <p style="margin-bottom: 15px;">{escape(message)}</p>
        <ol>
            <li>Go back to the Telegram bot</li>
            <li>Use <code>/getpremium</code> to get a new ad link</li>
            <li>Complete the ad verification</li>
            <li>You'll receive a valid code</li>
        </ol>
    </div>
    '''
    
    escaped_code = escape(code) if code else ''
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="robots" content="noindex, nofollow">
    <title>Verification {'Successful' if code else 'Failed'}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }}
        .container {{ background: white; border-radius: 20px; padding: 40px; max-width: 500px; width: 100%; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); text-align: center; }}
        .success-icon {{ font-size: 64px; margin-bottom: 20px; animation: scaleIn 0.5s ease-out; }}
        @keyframes scaleIn {{ from {{ transform: scale(0); opacity: 0; }} to {{ transform: scale(1); opacity: 1; }} }}
        h1 {{ color: #2d3748; margin-bottom: 15px; font-size: 28px; }}
        .message {{ color: #718096; margin-bottom: 30px; font-size: 16px; line-height: 1.6; }}
        .code-box {{ background: #f7fafc; border: 2px dashed #667eea; border-radius: 12px; padding: 20px; margin: 25px 0; }}
        .code-label {{ color: #4a5568; font-size: 14px; font-weight: 600; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px; }}
        .verification-code {{ font-size: 32px; font-weight: bold; color: #667eea; font-family: 'Courier New', monospace; letter-spacing: 4px; user-select: all; cursor: pointer; padding: 10px; background: white; border-radius: 8px; transition: all 0.3s ease; }}
        .verification-code:hover {{ background: #edf2f7; transform: scale(1.05); }}
        .timer-warning {{ background: #fef5e7; border: 1px solid #f39c12; border-radius: 8px; padding: 12px; margin: 15px 0; color: #856404; font-size: 14px; }}
        .instructions {{ background: #edf2f7; border-radius: 12px; padding: 20px; text-align: left; margin-top: 25px; }}
        .instructions h3 {{ color: #2d3748; font-size: 18px; margin-bottom: 15px; }}
        .instructions ol {{ color: #4a5568; padding-left: 20px; line-height: 1.8; }}
        .instructions code {{ background: white; padding: 2px 8px; border-radius: 4px; font-family: 'Courier New', monospace; color: #667eea; font-size: 14px; }}
        .btn {{ border: none; padding: 14px 32px; border-radius: 8px; font-size: 16px; font-weight: 600; cursor: pointer; margin: 10px 5px; transition: all 0.3s ease; display: inline-block; text-decoration: none; min-width: 200px; }}
        .btn-primary {{ background: #48bb78; color: white; }}
        .btn-primary:hover {{ background: #38a169; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(72, 187, 120, 0.4); }}
        .btn-secondary {{ background: #667eea; color: white; }}
        .btn-secondary:hover {{ background: #5568d3; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4); }}
        .btn.success {{ background: #48bb78; }}
        .alert {{ padding: 12px 16px; border-radius: 8px; margin: 15px 0; font-size: 14px; }}
        .alert-info {{ background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }}
        @media (max-width: 500px) {{
            .container {{ padding: 30px 20px; }}
            h1 {{ font-size: 24px; }}
            .verification-code {{ font-size: 24px; letter-spacing: 2px; }}
            .btn {{ min-width: 100%; margin: 8px 0; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="success-icon">{icon}</div>
        <h1>{escape(title)}</h1>
        <p class="message">{escape(message)}</p>
        <div id="alertContainer"></div>
        {code_section}
    </div>
    <script>
        const code = '{escaped_code}';
        const hasCode = code && code !== '';
        function copyCode() {{
            if (!code) return;
            const btn = document.getElementById('copyBtn');
            navigator.clipboard.writeText(code).then(() => {{
                btn.textContent = '✓ Copied!';
                btn.classList.add('success');
                setTimeout(() => {{ btn.textContent = '📋 Copy Code'; btn.classList.remove('success'); }}, 2000);
            }}).catch(() => {{
                const textArea = document.createElement('textarea');
                textArea.value = code;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
                btn.textContent = '✓ Copied!';
                setTimeout(() => {{ btn.textContent = '📋 Copy Code'; }}, 2000);
            }});
        }}
        function trackClick(action) {{ try {{ localStorage.setItem('last_action', action); }} catch(e) {{}} }}
        if (hasCode) {{
            let expiryTime = new Date(Date.now() + 30 * 60 * 1000);
            setInterval(() => {{
                const remaining = expiryTime - Date.now();
                if (remaining <= 0) {{ document.getElementById('timeRemaining').textContent = 'EXPIRED'; return; }}
                const minutes = Math.floor(remaining / 60000);
                const seconds = Math.floor((remaining % 60000) / 1000);
                document.getElementById('timeRemaining').textContent = `${{minutes}}:${{seconds.toString().padStart(2, '0')}}`;
            }}, 1000);
        }}
    </script>
</body>
</html>'''
    return html

def application(environ, start_response):
    """Minimal WSGI application"""
    path = environ.get('PATH_INFO', '/')
    method = environ.get('REQUEST_METHOD', 'GET')
    
    headers_common = [
        ('Cache-Control', 'no-cache, no-store, must-revalidate'),
        ('Pragma', 'no-cache'),
        ('Expires', '0')
    ]
    
    try:
        if path == '/':
            status = '200 OK'
            body = b'{"status": "online", "message": "Telegram Bot is running!", "bot": "Restricted Content Downloader"}'
            headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
            start_response(status, headers)
            return [body]
        
        elif path == '/health':
            status = '204 No Content'
            headers = headers_common
            start_response(status, headers)
            return [b'']
        
        elif path == '/memory-debug':
            try:
                from memory_monitor import memory_monitor
                import json
                from datetime import datetime
                
                # Get current memory state and log it to file
                mem_data = memory_monitor.get_memory_state_for_endpoint()
                
                # Format as pretty JSON
                status = '200 OK'
                body = json.dumps(mem_data, indent=2).encode('utf-8')
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
            except Exception as e:
                status = '500 Internal Server Error'
                body = f'{{"error": "{escape(str(e))}"}}'.encode('utf-8')
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
        
        elif path == '/verify-ad' and method == 'GET':
            from ad_monetization import ad_monetization
            from config import PyroConf
            from logger import LOGGER
            
            query_string = environ.get('QUERY_STRING', '')
            params = parse_qs(query_string)
            session_id = params.get('session', [''])[0].strip()
            confirm = params.get('confirm', [''])[0].strip()
            
            # Log all verification attempts for debugging
            LOGGER(__name__).info(f"Received /verify-ad request with session: {session_id[:16] if session_id else 'empty'}... | confirm={confirm}")
            
            if not session_id:
                html = load_template('', 'Invalid Request', 'No session ID provided. Please use the link from /getpremium command.', PyroConf.BOT_USERNAME or '')
            elif confirm != '1':
                # Show landing page - prevents shortener services from triggering code generation
                LOGGER(__name__).info(f"Showing landing page for session {session_id[:16]}... (no confirm parameter)")
                html = load_landing_page(session_id)
            else:
                # User clicked "Continue" button - now generate the code
                success, code, message = ad_monetization.verify_ad_completion(session_id)
                
                if success:
                    LOGGER(__name__).info(f"✅ Ad verification SUCCESS for session {session_id[:16]}... | Code: {code}")
                    html = load_template(code, 'Ad Completed Successfully! 🎉', 'Congratulations! You have successfully completed the ad verification.', PyroConf.BOT_USERNAME or '')
                else:
                    LOGGER(__name__).warning(f"❌ Ad verification FAILED for session {session_id[:16] if session_id else 'empty'}... | Reason: {message}")
                    html = load_template('', 'Verification Failed', message, PyroConf.BOT_USERNAME or '')
            
            status = '200 OK'
            body = html.encode('utf-8')
            headers = [
                ('Content-Type', 'text/html; charset=utf-8'),
                ('X-Content-Type-Options', 'nosniff'),
                ('X-Frame-Options', 'DENY')
            ] + headers_common
            start_response(status, headers)
            return [body]
        
        elif path == '/files' and method == 'GET':
            import os
            from datetime import datetime
            
            try:
                files_list = []
                base_dir = os.getcwd()
                
                for root, dirs, files in os.walk(base_dir):
                    dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules', '.git']]
                    
                    for filename in files:
                        if filename.startswith('.'):
                            continue
                        
                        filepath = os.path.join(root, filename)
                        rel_path = os.path.relpath(filepath, base_dir)
                        
                        try:
                            stat = os.stat(filepath)
                            size = stat.st_size
                            modified = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                            
                            if size < 1024:
                                size_str = f"{size} B"
                            elif size < 1024 * 1024:
                                size_str = f"{size / 1024:.1f} KB"
                            else:
                                size_str = f"{size / (1024 * 1024):.1f} MB"
                            
                            files_list.append({
                                'name': rel_path,
                                'size': size_str,
                                'size_bytes': size,
                                'modified': modified
                            })
                        except:
                            continue
                
                files_list.sort(key=lambda x: x['size_bytes'], reverse=True)
                
                files_html = ''
                for f in files_list:
                    icon = '📄'
                    if f['name'].endswith('.db'):
                        icon = '🗄️'
                    elif f['name'].endswith('.log'):
                        icon = '📝'
                    elif f['name'].endswith('.py'):
                        icon = '🐍'
                    elif f['name'].endswith(('.txt', '.md')):
                        icon = '📋'
                    
                    files_html += f'''
                    <tr>
                        <td>{icon} {escape(f['name'])}</td>
                        <td>{escape(f['size'])}</td>
                        <td>{escape(f['modified'])}</td>
                        <td><a href="/download?file={escape(f['name'])}" class="download-btn">⬇️ Download</a></td>
                    </tr>'''
                
                html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>File Browser - Render Deployment</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }}
        .container {{ background: white; border-radius: 20px; padding: 30px; max-width: 1200px; margin: 0 auto; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); }}
        h1 {{ color: #2d3748; margin-bottom: 10px; font-size: 32px; }}
        .subtitle {{ color: #718096; margin-bottom: 30px; font-size: 16px; }}
        .stats {{ background: #f7fafc; border-radius: 12px; padding: 20px; margin-bottom: 30px; display: flex; gap: 30px; flex-wrap: wrap; }}
        .stat-item {{ flex: 1; min-width: 150px; }}
        .stat-label {{ color: #718096; font-size: 14px; margin-bottom: 5px; }}
        .stat-value {{ color: #2d3748; font-size: 24px; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }}
        thead {{ background: #667eea; color: white; }}
        th {{ padding: 15px; text-align: left; font-weight: 600; }}
        td {{ padding: 12px 15px; border-bottom: 1px solid #e2e8f0; }}
        tr:hover {{ background: #f7fafc; }}
        .download-btn {{ background: #48bb78; color: white; padding: 6px 16px; border-radius: 6px; text-decoration: none; font-size: 14px; font-weight: 600; transition: all 0.3s; display: inline-block; }}
        .download-btn:hover {{ background: #38a169; transform: translateY(-2px); }}
        .refresh-btn {{ background: #667eea; color: white; padding: 12px 24px; border: none; border-radius: 8px; font-size: 16px; font-weight: 600; cursor: pointer; margin-bottom: 20px; transition: all 0.3s; }}
        .refresh-btn:hover {{ background: #5568d3; transform: translateY(-2px); }}
        @media (max-width: 768px) {{
            table {{ font-size: 14px; }}
            th, td {{ padding: 10px 8px; }}
            .stats {{ flex-direction: column; gap: 15px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📁 File Browser</h1>
        <p class="subtitle">Render Deployment - All accessible files</p>
        
        <div class="stats">
            <div class="stat-item">
                <div class="stat-label">Total Files</div>
                <div class="stat-value">{len(files_list)}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Total Size</div>
                <div class="stat-value">{sum(f['size_bytes'] for f in files_list) / (1024*1024):.1f} MB</div>
            </div>
        </div>
        
        <button class="refresh-btn" onclick="location.reload()">🔄 Refresh List</button>
        
        <table>
            <thead>
                <tr>
                    <th>File Name</th>
                    <th>Size</th>
                    <th>Last Modified</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {files_html}
            </tbody>
        </table>
    </div>
</body>
</html>'''
                
                status = '200 OK'
                body = html.encode('utf-8')
                headers = [('Content-Type', 'text/html; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
                
            except Exception as e:
                status = '500 Internal Server Error'
                body = f'{{"error": "{escape(str(e))}"}}'.encode('utf-8')
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
        
        elif path == '/download' and method == 'GET':
            import os
            
            query_string = environ.get('QUERY_STRING', '')
            params = parse_qs(query_string)
            filename = params.get('file', [''])[0].strip()
            
            if not filename:
                status = '400 Bad Request'
                body = b'{"error": "No file specified"}'
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
            
            filepath = os.path.join(os.getcwd(), filename)
            
            if not os.path.exists(filepath) or not os.path.isfile(filepath):
                status = '404 Not Found'
                body = b'{"error": "File not found"}'
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
            
            if '..' in filename or filename.startswith('/'):
                status = '403 Forbidden'
                body = b'{"error": "Access denied"}'
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
            
            try:
                with open(filepath, 'rb') as f:
                    file_data = f.read()
                
                safe_filename = os.path.basename(filename)
                status = '200 OK'
                headers = [
                    ('Content-Type', 'application/octet-stream'),
                    ('Content-Disposition', f'attachment; filename="{safe_filename}"'),
                    ('Content-Length', str(len(file_data)))
                ] + headers_common
                start_response(status, headers)
                return [file_data]
                
            except Exception as e:
                status = '500 Internal Server Error'
                body = f'{{"error": "{escape(str(e))}"}}'.encode('utf-8')
                headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
                start_response(status, headers)
                return [body]
        
        else:
            status = '404 Not Found'
            body = b'{"error": "Not Found"}'
            headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
            start_response(status, headers)
            return [body]
    
    except Exception as e:
        from logger import LOGGER
        LOGGER(__name__).error(f"WSGI error on {path}: {e}")
        status = '500 Internal Server Error'
        body = b'{"error": "Internal Server Error"}'
        headers = [('Content-Type', 'application/json; charset=utf-8')] + headers_common
        start_response(status, headers)
        return [body]

async def periodic_gc_task():
    """Periodic garbage collection for memory-constrained environments"""
    import gc
    import asyncio
    from memory_monitor import memory_monitor
    
    while True:
        try:
            await asyncio.sleep(300)
            collected = gc.collect()
            if collected > 0:
                from logger import LOGGER
                LOGGER(__name__).debug(f"Garbage collection freed {collected} objects")
                memory_monitor.log_memory_snapshot("Garbage Collection", f"Freed {collected} objects")
        except Exception as e:
            from logger import LOGGER
            LOGGER(__name__).error(f"Garbage collection error: {e}")

async def cleanup_watchdog_task():
    """Cleanup watchdog to prevent memory leaks from ad sessions and orphaned queue items.
    Runs every 5 minutes to purge:
    1. Expired ad sessions (>30 min old) and their cache entries
    2. Stale queue items that have been waiting too long
    3. Orphaned download tasks that failed to clean up properly
    """
    import asyncio
    from logger import LOGGER
    
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            
            # Clean up expired ad sessions
            try:
                from database_sqlite import db
                cleanup_result = db.cleanup_expired_sessions()
                if cleanup_result['sessions'] > 0 or cleanup_result['verifications'] > 0:
                    LOGGER(__name__).info(
                        f"🧹 Cleanup watchdog: removed {cleanup_result['sessions']} expired ad sessions "
                        f"and {cleanup_result['verifications']} verification codes"
                    )
            except Exception as e:
                LOGGER(__name__).error(f"Error in ad sessions cleanup: {e}")
            
            # Clean up stale queue items and orphaned tasks
            try:
                from queue_manager import download_queue
                sweep_result = await download_queue.sweep_stale_items(max_age_minutes=30)
                if sweep_result['stale_items'] > 0 or sweep_result['orphaned_tasks'] > 0:
                    LOGGER(__name__).warning(
                        f"🧹 Cleanup watchdog: removed {sweep_result['stale_items']} stale queue items "
                        f"and {sweep_result['orphaned_tasks']} orphaned tasks"
                    )
            except Exception as e:
                LOGGER(__name__).error(f"Error in queue cleanup: {e}")
            
            # Log memory snapshot after cleanup
            from memory_monitor import memory_monitor
            memory_monitor.log_memory_snapshot("Cleanup Watchdog", "After cleanup sweep")
            
        except Exception as e:
            from logger import LOGGER
            LOGGER(__name__).error(f"Cleanup watchdog error: {e}")

def run_bot():
    """Run the Telegram bot in a background thread with long polling"""
    import asyncio
    
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    import main
    
    async def start_bot():
        import sys
        # Initialize background tasks list before try block to avoid UnboundLocalError in finally
        background_tasks = []
        
        try:
            # CRITICAL: Cleanup orphaned files from previous crashes FIRST
            from helpers.files import cleanup_orphaned_files
            files_removed, bytes_freed = cleanup_orphaned_files()
            if files_removed > 0:
                main.LOGGER(__name__).warning(
                    f"🧹 Startup cleanup: Removed {files_removed} orphaned files "
                    f"({bytes_freed / (1024*1024):.1f} MB freed) from previous crashes"
                )
            
            main.LOGGER(__name__).info("Starting Telegram bot from server_wsgi.py (long polling)")
            await main.bot.start(bot_token=main.PyroConf.BOT_TOKEN)
            
            import time
            main.bot.start_time = time.time()
            
            main.LOGGER(__name__).info("Bot started successfully, waiting for updates...")
            
            main.phone_auth_handler.start_cleanup_task()
            
            from helpers.cleanup import start_periodic_cleanup
            background_tasks.append(asyncio.create_task(start_periodic_cleanup(interval_minutes=30)))
            main.LOGGER(__name__).info("Started periodic download cleanup task")
            
            from helpers.session_manager import session_manager
            await session_manager.start_cleanup_task()
            main.LOGGER(__name__).info("Started periodic session cleanup task (30min idle timeout)")
            
            background_tasks.append(asyncio.create_task(periodic_gc_task()))
            main.LOGGER(__name__).info("Started periodic garbage collection task")
            
            background_tasks.append(asyncio.create_task(cleanup_watchdog_task()))
            main.LOGGER(__name__).info("Started cleanup watchdog task (removes expired ad sessions and stale queue items every 5 min)")
            
            from memory_monitor import memory_monitor
            background_tasks.append(asyncio.create_task(memory_monitor.periodic_monitor(interval=300)))
            main.LOGGER(__name__).info("Started periodic memory monitoring (5-minute intervals)")
            
            # Start download queue processor using asyncio (not threading)
            from queue_manager import download_queue
            await download_queue.start_processor()
            main.LOGGER(__name__).info("Download queue processor initialized")
            
            memory_monitor.log_memory_snapshot("Bot Startup", "Initial state after bot start")
            
            # Cloud backup tasks (GitHub backups every 10 minutes)
            try:
                from cloud_backup import periodic_cloud_backup, restore_latest_from_cloud
                cloud_service = main.PyroConf.CLOUD_BACKUP_SERVICE
                if cloud_service:
                    _logger.info(f"Cloud backup configured: {cloud_service}")
                    # Always restore from GitHub on startup (cloud-only approach)
                    _logger.info("Restoring database from GitHub...")
                    result = await restore_latest_from_cloud()
                    if result:
                        _logger.info(f"✅ Database restored from {cloud_service}")
                    else:
                        _logger.warning(f"No backup found or restoration failed")
                    
                    # Start periodic cloud backups (every 10 minutes)
                    cloud_interval_minutes = int(os.environ.get('CLOUD_BACKUP_INTERVAL_MINUTES', '10'))
                    background_tasks.append(asyncio.create_task(periodic_cloud_backup(interval_minutes=cloud_interval_minutes)))
                    _logger.info(f"Started periodic {cloud_service} backup (every {cloud_interval_minutes} minutes)")
                else:
                    _logger.info("Cloud backup not configured")
            except Exception as e:
                _logger.warning(f"Cloud backup error: {e}")
            
            # Periodic orphaned file cleanup (every 1 hour) to prevent storage bloat from crashes
            async def periodic_orphaned_cleanup():
                while True:
                    try:
                        await asyncio.sleep(3600)  # 1 hour
                        from helpers.files import cleanup_orphaned_files
                        files, bytes_freed = cleanup_orphaned_files()
                        if files > 0:
                            main.LOGGER(__name__).warning(
                                f"⏰ Periodic cleanup: Removed {files} orphaned files "
                                f"({bytes_freed / (1024*1024):.1f} MB freed)"
                            )
                    except Exception as e:
                        main.LOGGER(__name__).error(f"Periodic orphaned cleanup error: {e}")
            
            background_tasks.append(asyncio.create_task(periodic_orphaned_cleanup()))
            main.LOGGER(__name__).info("Started periodic orphaned file cleanup (every 1h)")
            
            _logger.info("About to verify dump channel...")
            
            try:
                await main.verify_dump_channel()
                _logger.info("Dump channel verification complete")
            except Exception as e:
                _logger.error(f"Error in verify_dump_channel: {e}")
            
            _logger.info("Bot is now running and listening for updates...")
            await main.bot.run_until_disconnected()
        finally:
            _logger.info("Bot shutting down gracefully...")
            
            # First, disconnect sessions and bot cleanly
            try:
                from helpers.session_manager import session_manager
                await session_manager.disconnect_all()
                main.LOGGER(__name__).info("Disconnected all user sessions")
            except Exception as e:
                main.LOGGER(__name__).error(f"Error disconnecting sessions: {e}")
            
            try:
                await main.bot.disconnect()
                main.LOGGER(__name__).info("Bot disconnected")
            except Exception as e:
                main.LOGGER(__name__).error(f"Error disconnecting bot: {e}")
            
            # Then cancel background tasks to prevent "Task was destroyed" errors
            try:
                if background_tasks:
                    _logger.info(f"Cancelling {len(background_tasks)} background tasks...")
                    for task in background_tasks:
                        if not task.done():
                            task.cancel()
                    # Wait for background tasks to finish cancellation
                    await asyncio.gather(*background_tasks, return_exceptions=True)
                    _logger.info("All background tasks cancelled successfully")
            except Exception as e:
                _logger.error(f"Error cancelling background tasks: {e}")
            
            main.LOGGER(__name__).info("Bot stopped")
    
    loop.run_until_complete(start_bot())

import threading
bot_started = False
bot_lock = threading.Lock()

def start_bot_once():
    """Start bot only once to prevent duplicate instances"""
    global bot_started
    with bot_lock:
        if not bot_started:
            _logger.info("Starting Telegram bot in background thread...")
            bot_thread = threading.Thread(target=run_bot, daemon=True)
            bot_thread.start()
            bot_started = True

start_bot_once()

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 5000))
    
    _logger.info(f"Starting Waitress WSGI server on 0.0.0.0:{port} (minimal RAM mode)")
    serve(application, host='0.0.0.0', port=port, threads=4, channel_timeout=60)
