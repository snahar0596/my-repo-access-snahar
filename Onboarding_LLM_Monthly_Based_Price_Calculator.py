import os


def calculate_monthly_cost(input_tokens_per_request,
                           output_tokens_per_request,
                           requests_per_second,
                           input_price_per_1k=0.00135,
                           output_price_per_1k=0.0054,
                           seconds_per_minute=60,
                           minutes_per_hour=60,
                           hours_per_day=8,
                           days_per_month=30):
    """
    Calculate the monthly cost for API usage based on token consumption and request rate.
    Returns:
        Dictionary containing detailed cost breakdown
    """

    # Converting requests per second to: rp minute, rp hour, rp day, rp month
    requests_per_minute = requests_per_second * seconds_per_minute
    requests_per_hour = requests_per_minute * minutes_per_hour
    requests_per_day = requests_per_hour * hours_per_day
    requests_per_month = requests_per_day * days_per_month


    # Calculate average tokens
    avg_tokens_per_request = input_tokens_per_request + output_tokens_per_request
    avg_tokens_per_second = avg_tokens_per_request * requests_per_second
    avg_tokens_per_minute = avg_tokens_per_request * requests_per_minute
    avg_tokens_per_hour = avg_tokens_per_request * requests_per_hour


    # Calculate monthly tokens
    monthly_input_tokens = (input_tokens_per_request * requests_per_minute * minutes_per_hour * hours_per_day * days_per_month)
    monthly_output_tokens = (output_tokens_per_request * requests_per_minute * minutes_per_hour * hours_per_day * days_per_month)
    monthly_total_tokens = monthly_input_tokens + monthly_output_tokens


    # Convert to millions
    monthly_input_millions = monthly_input_tokens / 1000000
    monthly_output_millions = monthly_output_tokens / 1000000
    monthly_total_millions = monthly_total_tokens / 1000000


    # Calculate costs
    input_cost = monthly_input_millions * input_price_per_1k * 1000
    output_cost = monthly_output_millions * output_price_per_1k * 1000
    total_cost = input_cost + output_cost


    return {
        'requests_per_second': requests_per_second,
        'requests_per_minute': requests_per_minute,
        'requests_per_hour': requests_per_hour,
        'requests_per_day': requests_per_day,
        'requests_per_month': requests_per_month,

        'input_tokens_per_request': input_tokens_per_request,
        'output_tokens_per_request': output_tokens_per_request,
        'avg_tokens_per_request': avg_tokens_per_request,

        'avg_tokens_per_second': avg_tokens_per_second,
        'avg_tokens_per_minute': avg_tokens_per_minute,
        'avg_tokens_per_hour': avg_tokens_per_hour,

        'monthly_input_tokens': monthly_input_tokens,
        'monthly_output_tokens': monthly_output_tokens,
        'monthly_total_tokens': monthly_total_tokens,

        'monthly_input_millions': monthly_input_millions,
        'monthly_output_millions': monthly_output_millions,
        'monthly_total_millions': monthly_total_millions,

        'input_cost_usd': input_cost,
        'output_cost_usd': output_cost,
        'total_cost_usd': total_cost,
        'input_price_per_1k': input_price_per_1k,
        'output_price_per_1k': output_price_per_1k
    }


def format_cost_breakdown(results):
    """Format the cost breakdown in a readable way"""

    print("\n" + "="*60)
    print("=" * 60)
    print("MONTHLY COST CALCULATION BREAKDOWN")
    print("=" * 60)
    print(f"Requests per second: {results['requests_per_second']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Requests per minute: {results['requests_per_minute']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Requests per hour: {results['requests_per_hour']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Requests per day: {results['requests_per_day']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Requests per month: {results['requests_per_month']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Input tokens per request: {results['input_tokens_per_request']}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Output tokens per request: {results['output_tokens_per_request']}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Average tokens per request: {results['avg_tokens_per_request']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Average tokens per second: {results['avg_tokens_per_second']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))    
    print(f"Average tokens per minute: {results['avg_tokens_per_minute']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Average tokens per hour: {results['avg_tokens_per_hour']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Monthly input tokens: {results['monthly_input_tokens']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Monthly output tokens: {results['monthly_output_tokens']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Monthly total tokens: {results['monthly_total_tokens']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Monthly input tokens (millions): {results['monthly_input_millions']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Monthly output tokens (millions): {results['monthly_output_millions']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Monthly total tokens (millions): {results['monthly_total_millions']:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Input cost: ${results['input_cost_usd']:,.4f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Output cost: ${results['output_cost_usd']:,.4f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print("=" * 60)
    print(f"TOTAL MONTHLY COST: ${results['total_cost_usd']:,.4f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print("=" * 60)


if __name__ == "__main__":

    want_to_continue = True

    while(bool(want_to_continue) == True):
        os.system('cls')
        input_tokens_per_request = float(eval(input('input_tokens_per_request:  ')))
        output_tokens_per_request = float(eval(input('output_tokens_per_request:  ')))
        requests_per_second = float(eval(input('requests_per_second:  ')))
        input_price_per_1k = float(eval(input('input_price_per_1k, (0.00135):  ')))  # 0.00135
        output_price_per_1k = float(eval(input('output_price_per_1k, (0.0054):  ')))  # 0.0054


        results = calculate_monthly_cost(
            input_tokens_per_request=input_tokens_per_request,
            output_tokens_per_request=output_tokens_per_request,
            requests_per_second=requests_per_second,
            input_price_per_1k=input_price_per_1k,
            output_price_per_1k=output_price_per_1k,
        )

        format_cost_breakdown(results)

        want_to_continue = str(input('\n\nDo you want to calculate again?:  '))


    # Higher volume
    # results1 = calculate_monthly_cost(
    #     input_tokens_per_request=100,
    #     output_tokens_per_request=200,
    #     requests_per_second=5  # 5 requests per second
    # )


    # Lower volume with more tokens
    # results2 = calculate_monthly_cost(
    #     input_tokens_per_request=1000,
    #     output_tokens_per_request=2000,
    #     requests_per_second=0.1  # 0.1 requests per second
    # )