import createServer from './server';

const PORT = parseInt(process.env.PORT || '5501', 10);

createServer()
    .then((app) => {
        app.listen(PORT, () => {
            console.log(`pdf-service listening on :${PORT}`);
        });
    })
    .catch((err) => {
        console.error('failed to start pdf-service:', err);
        process.exit(1);
    });
