const axios = require('axios');

async function authenticate() {
    try {
        const response = await axios.post('http://179.51.237.14:8081/web/api/chess/v1/auth/login', {
            usuario: 'delpalacioapi',
            password: '1234'
        }, {
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        console.log("Estado de respuesta:", response.status);
        console.log("Contenido de la respuesta:", response.data);

        if (response.status === 200 && response.data.sessionId) {
            const sessionId = response.data.sessionId.replace('JSESSIONID=', '');
            console.log('Session ID obtenido:', sessionId);
            return sessionId;
        } else {
            console.log('Error en la autenticación:', response.data);
            return null;
        }
    } catch (error) {
        console.error('Error:', error.response ? error.response.data : error.message);
        return null;
    }
}

async function getPedidos(sessionId, version) {
    try {
        const endpoint = `http://179.51.237.14:8081/web/api/chess/${version}/pedidos`;
        const pedidosResponse = await axios.get(endpoint, {
            headers: {
                'Cookie': `JSESSIONID=${sessionId}`,
                'Accept': 'application/json'
            },
            params: {
                pedido: '',  // Ajustar según los parámetros necesarios para el endpoint de pedidos
                anulado: 'false'  // Por ejemplo, si deseas filtrar por pedidos no anulados
            }
        });

        if (pedidosResponse.status === 200) {
            console.log('Pedidos obtenidos:', pedidosResponse.data);
        } else {
            console.log('Error al obtener pedidos:', pedidosResponse.data);
        }
    } catch (error) {
        console.error('Error al obtener pedidos:', error.response ? error.response.data : error.message);
    }
}

async function main() {
    const sessionId = await authenticate();
    if (sessionId) {
        const versions = ['v1'];  // Probar con diferentes versiones
        for (const version of versions) {
            console.log(`\nProbando con versión: ${version}`);
            await getPedidos(sessionId, version);
        }
    }
}

main();




