/// <reference lib="webworker" />

import type { BackgroundSetupOptions, DownloadMetadata } from './types';

declare const self: ServiceWorkerGlobalScope;

/** Bytes the browser may take between two progress reports to the page. */
const ACK_EVERY_BYTES = 1024 * 1024;

/**
 * A stream that hands the browser one chunk each time it asks for one,
 * and tells the page how many bytes the browser has taken so far
 * (`{ consumed }`) and when it has taken the last one (`{ done: true }`).
 * The page uses that to hold its writes while the disk is behind, and
 * to end close() only when the download has all of its data.
 */
export function createAckedStream(
  port: MessagePort,
  log: (...args: unknown[]) => void = () => {}
): ReadableStream<Uint8Array> {
  const queue: Uint8Array[] = [];
  let ended = false;
  let finished = false;
  let consumed = 0;
  let reported = 0;
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let waiting: (() => void) | null = null;

  const settle = () => {
    const resolve = waiting;
    waiting = null;
    if (resolve) resolve();
  };

  const pump = () => {
    if (!waiting || finished || !controller) return;
    if (queue.length > 0) {
      const chunk = queue.shift()!;
      consumed += chunk.byteLength;
      controller.enqueue(chunk);
      if (queue.length === 0 || consumed - reported >= ACK_EVERY_BYTES) {
        reported = consumed;
        port.postMessage({ consumed });
      }
      settle();
    } else if (ended) {
      finished = true;
      log('Stream ended');
      controller.close();
      port.postMessage({ consumed, done: true });
      settle();
    }
  };

  return new ReadableStream<Uint8Array>(
    {
      start(c) {
        controller = c;
        port.onmessage = ({ data: messageData }) => {
          if (finished) return;
          if (messageData === 'end') {
            ended = true;
          } else if (messageData === 'abort') {
            finished = true;
            queue.length = 0;
            log('Stream aborted');
            c.error('Download aborted');
            settle();
            return;
          } else {
            queue.push(messageData);
          }
          pump();
        };
      },
      pull() {
        return new Promise<void>((resolve) => {
          waiting = resolve;
          pump();
        });
      },
      cancel(reason) {
        finished = true;
        queue.length = 0;
        log('Stream cancelled:', reason);
        port.postMessage({ abort: true });
      },
    },
    { highWaterMark: 0 }
  );
}

/**
 * Sets up streaming download support in a service worker.
 *
 * For **web apps** (non-extension), call this in your service worker to handle
 * download message/fetch events directly without needing the bridge files.
 *
 * For **browser extensions**, this is NOT required — the bridge files
 * (bridge.html, bridge.js, sw.js) handle everything via a dedicated SW.
 * Just copy the bridge files to your extension and add them to
 * web_accessible_resources.
 *
 * @param options - Optional configuration
 *
 * @example
 * ```typescript
 * // service-worker.js (web app only)
 * import { setupStreamingDownloads } from 'drip-fs/background';
 *
 * setupStreamingDownloads({ debug: true });
 * ```
 */
export function setupStreamingDownloads(options: BackgroundSetupOptions = {}): void {
  const { debug = false } = options;

  const log = (...args: unknown[]) => {
    if (debug) {
      console.log('[drip-fs]', ...args);
    }
  };

  // Map to track active downloads
  const downloadMap = new Map<string, DownloadMetadata>();

  // Service worker lifecycle - claim clients immediately
  self.addEventListener('install', () => {
    log('Service worker installing');
    self.skipWaiting();
  });

  self.addEventListener('activate', (event: ExtendableEvent) => {
    log('Service worker activating');
    event.waitUntil(self.clients.claim());
  });

  // Store original message handler
  const originalOnMessage = self.onmessage;

  // Handle messages from bridge
  self.onmessage = (event: ExtendableMessageEvent) => {
    // Check if this is a drip-fs message (has ports)
    if (!event.ports || event.ports.length === 0) {
      // Not our message - call original handler
      if (originalOnMessage) {
        originalOnMessage.call(self, event);
      }
      return;
    }

    // Ignore ping messages
    if (event.data === 'ping') {
      return;
    }

    const data = event.data;
    const port = event.ports[0];

    // Generate download URL
    const baseScope = self.registration?.scope ?? `${self.location.origin}/`;
    const downloadUrl =
      data.url ||
      `${baseScope}${Math.random().toString().slice(2)}/${
        typeof data === 'string' ? data : data.filename
      }`;

    log('Registering download:', downloadUrl);

    // Create metadata entry
    const metadata: DownloadMetadata = {
      stream: null,
      data,
      port,
    };

    // A page that asks for it gets progress reports and a wait on its writes
    const acked = !!(data && (data as { ack?: boolean }).ack);

    // Handle different stream transfer methods
    if (event.data.readableStream) {
      metadata.stream = event.data.readableStream;
    } else if (event.data.transferringReadable) {
      port.onmessage = (evt: MessageEvent) => {
        port.onmessage = null;
        metadata.stream = evt.data.readableStream;
      };
    } else if (acked) {
      metadata.stream = createAckedStream(port, log);
    } else {
      metadata.stream = new ReadableStream({
        start(controller) {
          port.onmessage = ({ data: messageData }) => {
            if (messageData === 'end') {
              log('Stream ended');
              return controller.close();
            }

            if (messageData === 'abort') {
              log('Stream aborted');
              controller.error('Download aborted');
              return;
            }

            controller.enqueue(messageData);
          };
        },
        cancel(reason) {
          log('Stream cancelled:', reason);
          port.postMessage({ abort: true });
        },
      });
    }

    downloadMap.set(downloadUrl, metadata);
    port.postMessage(acked ? { download: downloadUrl, ack: true } : { download: downloadUrl });
  };

  // Handle fetch requests for downloads.
  // Uses onfetch property assignment (not addEventListener) for Firefox
  // event page compatibility — Firefox treats background scripts with
  // onfetch as having service worker fetch interception capability.
  (self as unknown as { onfetch: ((event: FetchEvent) => void) | null }).onfetch = (event: FetchEvent) => {
    const url = event.request.url;

    if (url.endsWith('/ping')) {
      event.respondWith(new Response('pong'));
      return;
    }

    const metadata = downloadMap.get(url);
    if (!metadata) {
      return;
    }

    log('Serving download:', url);
    downloadMap.delete(url);

    const { stream, data, port } = metadata;

    const headers = new Headers({
      'Content-Type': 'application/octet-stream; charset=utf-8',
      'Content-Security-Policy': "default-src 'none'",
      'X-Content-Security-Policy': "default-src 'none'",
      'X-WebKit-CSP': "default-src 'none'",
      'X-XSS-Protection': '1; mode=block',
    });

    const dataHeaders = new Headers((data as { headers?: HeadersInit }).headers || {});
    if (dataHeaders.has('Content-Length')) {
      headers.set('Content-Length', dataHeaders.get('Content-Length')!);
    }
    if (dataHeaders.has('Content-Disposition')) {
      headers.set('Content-Disposition', dataHeaders.get('Content-Disposition')!);
    }

    event.respondWith(new Response(stream, { headers }));
    port.postMessage({ debug: 'Download started' });
  };

  log('Streaming downloads initialized');
}
